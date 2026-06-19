import logging
import threading
import time

from .transport import Transport
from .server import GameServer
from .client import GameClient
from .discovery import Discovery
from .health import HealthMonitor
from .state import NetworkState
from .config import NetworkConfig
from .constants import MessageType

logger = logging.getLogger(__name__)

class NetworkManager:
    """Fachada orquestadora de la red (interfaz pública)."""
    
    def __init__(self):
        self.config = NetworkConfig()
        self.state = NetworkState()
        self.transport = Transport(self.config)
        self._server = GameServer(self.state, self.transport, self.config)
        self.client = GameClient(self.state, self.transport, self.config)
        self.discovery = Discovery(self.state, self.config)
        self.health = HealthMonitor(self.state, self.transport, self.config)
    
    # === Métodos públicos (INTERFAZ COMPATIBLE) ===
    
    def start_server(self, nameHost, password, max_players, nameSala="Sala Local"):
        """Inicia servidor.
        
        nameHost: nombre del jugador host (se muestra en el juego)
        nameSala: nombre de la sala/partida (se anuncia en LAN)
        """
        success = self._server.start(nameSala, nameHost, max_players, nameSala)
        if success:
            self.state.password = password
            self.state.playerName = nameHost   # Para que ui.py pueda leerlo
            self.state.running_broadcast = True
            self.discovery.start_broadcast()
            self.health.start_health_check()
        return success
    
    def connectToServer(self, server):
        """Conecta a un servidor."""
        result = self.client.connect(server)
        if result[0]:  # success
            self._current_server = server
        return result
    
    def sendData(self, data):
        """Envía datos al servidor en un hilo separado para no bloquear
        el hilo principal de pygame si el socket del Host está taponado."""
        def _do_send():
            self.client.send(data)
        threading.Thread(target=_do_send, daemon=True).start()
        return True
    
    def discoverServers(self, timeout=5):
        """Descubre servidores en la red local asíncronamente."""
        self.discovery.discover_servers(timeout)
        return None
    
    @property
    def servers(self):
        """Lista asíncrona de servidores para compatibilidad con UI."""
        return self.discovery.discovered_servers
    
    def broadcast_message(self, message, on_done=None, max_retries=3, retry_delay=0.4):
        """Broadcast a todos los clientes (solo HOST).
        
        Se ejecuta en un hilo separado para no bloquear el hilo principal
        de pygame. Sin esto, si el socket de un cliente se tapona (porque
        ese cliente está ocupado en su propio frame de pygame y no lee a
        tiempo), send_atomic queda esperando el timeout completo del socket
        y congela toda la ventana del Host ("No responde" en Windows).
        Con esto, el envío ocurre en background y el render sigue su curso.

        FIX (congelamiento en cliente al cambiar de ronda): antes, si
        send_atomic fallaba (timeout de socket, error de pickle, etc.) el
        error solo se logueaba con `logging` (que sin configurar handlers
        no se ve en consola) y el mensaje se perdía para siempre. Como
        broadcast_message se llama una sola vez para enviar PLAYER_ORDER,
        si ese único intento fallaba el cliente quedaba esperando ese
        mensaje eternamente, bloqueado en sock.recv() — la pantalla se
        veía "congelada" aunque el hilo principal seguía vivo.

        Ahora: cada destinatario tiene reintentos (max_retries) con un
        pequeño delay entre intentos, los errores se imprimen siempre por
        consola (no solo logging) para que sea visible en producción, y
        opcionalmente se invoca on_done(success: bool, failed_ids: list)
        cuando termina, para que el llamador pueda decidir si reintentar
        el mensaje completo (ver send_critical_broadcast más abajo).
        """
        if not self.state.is_host:
            logger.warning("Solo el HOST puede hacer broadcast")
            if on_done:
                on_done(False, [])
            return

        def _do_broadcast():
            failed_to_send = []
            for player in self.state.get_connected_players():
                if player.is_host:
                    continue
                sent_ok = False
                last_exc = None
                for attempt in range(1, max_retries + 1):
                    try:
                        if self.transport.send_atomic(player.conn, message):
                            sent_ok = True
                            break
                        else:
                            last_exc = "send_atomic devolvió False (timeout/error de envío)"
                    except Exception as e:
                        last_exc = e
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                if not sent_ok:
                    msg_type = message.get("type") if isinstance(message, dict) else "?"
                    print(f"[RED][ERROR] No se pudo enviar mensaje '{msg_type}' a "
                          f"{player.name} tras {max_retries} intentos. Último error: {last_exc}")
                    logger.error(f"Error broadcast a {player.name}: {last_exc}")
                    failed_to_send.append(player.player_id)
                    # FIX BUG CRÍTICO: antes, aquí se llamaba a
                    # remove_connected_player(player.player_id) por el solo
                    # hecho de fallar el envío (ej. timeout transitorio de
                    # socket). Eso expulsaba al jugador de la lista de
                    # conectados aunque su conexión TCP siguiera viva,
                    # rompiendo además los reintentos de
                    # send_critical_broadcast (en el siguiente intento ya
                    # no aparecía en get_connected_players() y el broadcast
                    # se reportaba "exitoso" sin haberle llegado nada a
                    # nadie). La desconexión real del jugador ya se detecta
                    # de forma fiable en server.py:_handle_player cuando
                    # recv_atomic devuelve None o lanza una excepción de
                    # socket cerrado/reseteado — no hace falta ni es
                    # correcto duplicar esa lógica aquí.

            if on_done:
                try:
                    on_done(len(failed_to_send) == 0, failed_to_send)
                except Exception as e:
                    logger.error(f"Error en callback on_done de broadcast_message: {e}")

        threading.Thread(target=_do_broadcast, daemon=True).start()

    def send_critical_broadcast(self, message, max_attempts=5, retry_delay=1.0):
        """Envía un mensaje crítico (ej: PLAYER_ORDER) garantizando reintento
        automático si el primer intento falla para algún jugador.

        A diferencia de broadcast_message (que es "fire and forget"), este
        método mantiene un hilo de vigilancia que reintenta el envío completo
        hasta max_attempts veces si quedó algún jugador sin recibir el
        mensaje. Esto evita que un fallo puntual de red deje a un cliente
        esperando para siempre un mensaje que nunca llegó (causa raíz del
        congelamiento al cambiar de ronda).
        """
        if not self.state.is_host:
            logger.warning("Solo el HOST puede hacer broadcast crítico")
            return

        def _attempt(n):
            def _on_done(success, failed_ids):
                if success:
                    print(f"[RED] Broadcast crítico '{message.get('type')}' entregado "
                          f"correctamente (intento {n}/{max_attempts}).")
                    return
                if n >= max_attempts:
                    print(f"[RED][ERROR] Broadcast crítico '{message.get('type')}' "
                          f"falló definitivamente tras {max_attempts} intentos. "
                          f"Jugadores afectados: {failed_ids}")
                    return
                print(f"[RED][AVISO] Reintentando broadcast crítico '{message.get('type')}' "
                      f"(intento {n+1}/{max_attempts}) para jugadores: {failed_ids}")
                time.sleep(retry_delay)
                _attempt(n + 1)

            self.broadcast_message(message, on_done=_on_done)

        threading.Thread(target=lambda: _attempt(1), daemon=True).start()
    
    def stop(self):
        """Detiene servidor y cliente."""
        self.state.running = False
        self.state.running_broadcast = False
        
        # Cerrar sockets
        if self._server.server_socket:
            try:
                self._server.server_socket.close()
            except:
                pass
        
        if self.state.player:
            try:
                self.state.player.close()
            except:
                pass
        
        logger.info("NetworkManager detenido")
    
    # === Getters de estado (INTERFAZ COMPATIBLE) ===
    
    def get_incoming_messages(self):
        return self.state.get_incoming_messages()
    
    def get_game_state(self):
        return self.state.get_game_state()
    
    def get_moves_game(self):
        return self.state.get_moves()
    
    def get_moves_gameServer(self):
        return self.state.get_moves(server=True)
    
    def canStartGame(self):
        return len(self.state.get_connected_players()) >= 2
    
    def startGame(self):
        self.state.game_started = True
        msg = {"type": MessageType.START_GAME.value}
        self.broadcast_message(msg)
        self.state.msgStartGame.update(msg)

    def send_player_order(self, msg: dict):
        """Envía el mensaje PLAYER_ORDER (cambio de ronda) garantizando
        reintentos automáticos y guardando una copia para poder reenviarla
        si algún cliente reporta que nunca la recibió (REQUEST_RESYNC).

        Esta es la corrección principal del congelamiento: antes se usaba
        broadcast_message "fire and forget" una sola vez; si fallaba, el
        cliente quedaba esperando ese mensaje para siempre.
        """
        if not self.state.is_host:
            return
        self.state.set_last_player_order(msg)
        self.send_critical_broadcast(msg)

    def request_resync(self):
        """Llamado por un CLIENTE cuando lleva demasiado tiempo esperando
        el siguiente PLAYER_ORDER (pantalla "congelada" en fase de
        elección). Pide al Host que reenvíe el último estado de ronda."""
        if self.state.is_host:
            return
        msg = {
            "type": MessageType.REQUEST_RESYNC.value,
            "playerId": self.state.player_id,
        }
        self.sendData(msg)

    def handle_resync_request(self, requesting_player_id=None):
        """Llamado por el HOST al recibir un REQUEST_RESYNC: reenvía el
        último PLAYER_ORDER conocido a todos (es más simple y seguro que
        reenviar solo a uno, y no tiene efectos secundarios ya que el
        cliente que sí lo recibió simplemente lo vuelve a procesar)."""
        if not self.state.is_host:
            return
        last_msg = self.state.get_last_player_order()
        if last_msg is not None:
            print(f"[RED] Reenviando último PLAYER_ORDER por solicitud de "
                  f"resync (jugador {requesting_player_id}).")
            self.send_critical_broadcast(last_msg)
        else:
            print("[RED][AVISO] Se pidió resync pero el Host no tiene "
                  "ningún PLAYER_ORDER guardado todavía.")

    # Propiedades para compatibilidad con llamadas viejas
    @property
    def is_host(self):
        return self.state.is_host
    
    @property
    def is_connected(self):
        return self.state.is_connected
    
    @property
    def connected_players(self):
        # Mapea ConnectedPlayer a tupla para la compatibilidad con ui.py (conn, addr, name, id)
        return [(p.conn, p.addr, p.name, p.player_id) for p in self.state.get_connected_players()]
    
    @property
    def gameName(self):
        return self.state.gameName
    
    @property
    def host(self):
        return self.state.host
    
    @property
    def port(self):
        return self.state.port
    
    @property
    def running(self):
        return self.state.running
    
    @running.setter
    def running(self, value):
        self.state.running = value

    @property
    def receive_thread_running(self):
        return self.state.running
    
    @property
    def player_id(self):
        return self.state.player_id
    
    @property
    def msgStartGame(self):
        return self.state.msgStartGame
    
    @msgStartGame.setter
    def msgStartGame(self, value):
         self.state.msgStartGame = value

    @property
    def game_started(self):
        return self.state.game_started
        
    @game_started.setter
    def game_started(self, value):
        self.state.game_started = value
         
    # --- Propiedades adiccionales para ui.py ---
    @property
    def lock(self):
        return self.state._lock_messages

    @property
    def receivedData(self):
        return self.state.receivedData
        
    @receivedData.setter
    def receivedData(self, value):
        self.state.receivedData = value

    @property
    def messagesServer(self):
        return self.state.messagesServer
        
    @property
    def server(self):
        # Exponemos el socket del Host para la validacion 'if self.network_manager.server:'
        return self._server.server_socket
        
    @property
    def player(self):
        # Exponemos el socket del Cliente
        return self.state.player
        
    @property
    def playerName(self):
        return self.state.playerName
        
    def stop_broadcast(self):
        self.state.running_broadcast = False
        
    @property
    def currentServer(self):
        """Devuelve info del server actual si es host, o el server seleccionado si es cliente."""
        if self.state.is_host:
            return {
                'name': self.state.gameName,
                'playerName': self.state.playerName,
                'ip': self.state.host,
                'port': self.state.port,
                'max_players': self.state.max_players,
                'password': self.state.password,
                'currentPlayers': len(self.state.get_connected_players())
            }
        return getattr(self, '_current_server', None)
    
    @property
    def mensaje(self):
        return getattr(self.state, 'mensaje', '')

    @property
    def tiempoDelMensaje(self):
        return getattr(self.state, 'tiempoDelMensaje', 0)

    def get_exit_gameServer(self):
        """Devuelve y borra la lista de mensajes de salir/desconexion del juego."""
        # TODO: Implementar estado real si ui2.py lo demanda, por ahora lista vacía
        return []

    def send_selection_update(self, cartas_eleccion_serializada):
        """El Host usa este método para notificar a todos la lista actualizada de cartas_eleccion."""
        if not self.state.is_host:
            logger.error("Solo el Host puede enviar actualizaciones de selección.")
            return

        message = {
            "type": MessageType.SELECTION_UPDATE.value,
            "cartas_eleccion": cartas_eleccion_serializada 
        }
        self.broadcast_message(message)

    def exit_game(self, playerId, playerName):
        msgSalir = {
            "type": "SALIR",
            "playerId": playerId,
            "playerName": playerName
        }
        self.sendData(msgSalir)

    def get_game_info(self):
        """Obtiene información del juego."""
        return {
            "gameName": self.state.gameName,
            "host": self.state.host,
            "port": self.state.port,
            "max_players": self.state.max_players,
            "connected_players": self.connected_players,
            "is_host": self.state.is_host
        }

    def dprint(self, dic):
        """Para imprimir mas bonito un diccionario."""
        if type(dic) == dict:
            for clave, valor in dic.items():
                print(f"{str(clave).rjust(15)}: {valor}")
        else:
            return False
    # === GESTOR DE CHAT Y NOTIFICACIONES ===

    # === GESTOR DE CHAT Y NOTIFICACIONES ===

    def send_chat_message(self, mensaje: str):
        """Estructura el JSON del chat y lo envía a la red."""
        msg_data = {
            "type": "CHAT",
            "playerName": self.state.playerName, 
            "mensaje": mensaje,
            "notificar": True # Flag de aviso para los receptores
        }
        
        if self.state.is_host:
            # CORRECCIÓN 1: Cambiamos el nombre del Host por "Tú" para su propia UI
            msgFormat = f"Tú: {mensaje}"
            
            # CORRECCIÓN 2: Imprimir directamente en la terminal del Servidor
            print(f"\n[CHAT - LOCAL (HOST)] Tú: {mensaje}")
            
            with self.state._lock_messages:
                self.state.messagesServer.append(msgFormat)
                if len(self.state.messagesServer) > 20:
                    self.state.messagesServer.pop(0)
            
            # Y luego lo retransmite al resto de jugadores
            self.broadcast_message(msg_data)
        else:
            # Los clientes normales simplemente se lo envían al Host
            self.sendData(msg_data)

    @property
    def needs_chat_notification(self) -> bool:
        """La UI puede consultar esta propiedad en cada frame para dibujar el ícono."""
        return self.state.has_unread_chat
        
    def clear_chat_notification(self):
        """Llama a este método justo en el evento donde el jugador abre el chat."""
        self.state.has_unread_chat = False