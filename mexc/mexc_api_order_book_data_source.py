import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS, mexc_web_utils as web_utils, mexc_utils
from hummingbot.connector.exchange.mexc.mexc_order_book import MexcOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange


class MexcAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'MexcExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "1000"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
            headers={"Content-Type": "application/json"}
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_params: List[str] = []
            depth_params: List[str] = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)  # e.g. BTC-USDT
                ex_symbol = mexc_utils.to_exchange_symbol(symbol)  # -> BTCUSDT
                trade_params.append(web_utils.topic_aggr_deals(ex_symbol, 100))
                depth_params.append(web_utils.topic_aggr_depth(ex_symbol, 100))

            await ws.send(WSJSONRequest(payload={
                "method": "SUBSCRIPTION",
                "params": trade_params,
                "id": self.TRADE_STREAM_ID
            }))
            await ws.send(WSJSONRequest(payload={
                "method": "SUBSCRIPTION",
                "params": depth_params,
                "id": self.DIFF_STREAM_ID
           }))
            # ping initial (et tu peux programmer un ping périodique côté task si besoin)
            await ws.send(WSJSONRequest(payload={"method": "PING"}))

            self.logger().info("Subscribed to trades & depth channels (Spot V3 Protobuf).")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = MexcOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, wrapper: ws_pb2.PushDataV3ApiWrapper, message_queue: asyncio.Queue):
         payload = ws_pb2.PublicDeals()
         payload.ParseFromString(wrapper.payload)
         symbol = wrapper.channel.split("@")[-1]   # ...@100ms@BTCUSDT
         trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
         for deal in payload.dealsList:
             trade_message = MexcOrderBook.trade_message_from_protobuf(
                 deal, metadata={"trading_pair": trading_pair}
             )
             message_queue.put_nowait(trade_message)
 

    async def _parse_order_book_diff_message(self, wrapper: ws_pb2.PushDataV3ApiWrapper, message_queue: asyncio.Queue):
        payload = ws_pb2.PublicAggreDepths()
        payload.ParseFromString(wrapper.payload)
        symbol = wrapper.channel.split("@")[-1]   # ...@100ms@BTCUSDT
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        bids = [(d.price, d.volume) for d in payload.bidsList]
        asks = [(d.price, d.volume) for d in payload.asksList]
        ob_message: OrderBookMessage = MexcOrderBook.diff_message_from_protobuf(
            bids=bids, asks=asks, update_id=payload.toVersion, metadata={"trading_pair": trading_pair}
        )
        message_queue.put_nowait(ob_message)

    def _channel_originating_message(self, wrapper: ws_pb2.PushDataV3ApiWrapper) -> str:
         channel = wrapper.channel
         if "aggre.depth.v3.api.pb" in channel:
             return self._diff_messages_queue_key
         elif "aggre.deals.v3.api.pb" in channel:
             return self._trade_messages_queue_key
         return ""

    async def _process_websocket_messages(self, ws: WSAssistant) -> None:
        """
        Lit les frames binaires protobuf et route vers les parseurs.
        """
        async for raw in ws.iter_raw():
            # On s'attend à des frames binaires (wrapper protobuf). Certains serveurs envoient parfois du PONG texte.
            if not isinstance(raw, (bytes, bytearray)):
                continue
            wrapper = ws_pb2.PushDataV3ApiWrapper()
            try:
                wrapper.ParseFromString(raw)
            except Exception:
                self.logger().debug("Failed to parse protobuf wrapper", exc_info=True)
                continue

            try:
                queue_key = self._channel_originating_message(wrapper)
                if queue_key == self._diff_messages_queue_key:
                    await self._parse_order_book_diff_message(wrapper, self._message_queue[self._diff_messages_queue_key])
                elif queue_key == self._trade_messages_queue_key:
                    await self._parse_trade_message(wrapper, self._message_queue[self._trade_messages_queue_key])
            except Exception:
                self.logger().warning(f"Error processing channel={wrapper.channel}", exc_info=True)
