from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from decimal import Decimal
import time
from typing import Any, List, Tuple, Dict, Optional
from .proto import ws_pb2

class MexcOrderBook(OrderBook):
    @staticmethod
    def trade_message_from_protobuf(deal: "ws_pb2.PublicDeals.Deal", metadata: Dict[str, Any]) -> OrderBookMessage:
        # Champs habituels côté MEXC protobuf: deal.id, deal.price, deal.volume, deal.dealType (BUY/SELL enum), deal.ts
        trade_type = TradeType.BUY if deal.dealType == ws_pb2.BUY else TradeType.SELL
        return OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trade_id": str(deal.id),
                "price": Decimal(str(deal.price)),
                "amount": Decimal(str(deal.volume)),
                "trade_type": trade_type,
            },
            timestamp=deal.ts / 1000.0,
            metadata=metadata,
        )

    @staticmethod
    def diff_message_from_protobuf(bids: List[Tuple[float, float]], asks: List[Tuple[float, float]],
                                   update_id: int, metadata: Dict[str, Any]) -> OrderBookMessage:
        # Convertit vers Decimals pour rester conforme au reste d’HB
        content = {
            "bids": [(Decimal(str(p)), Decimal(str(q))) for p, q in bids],
            "asks": [(Decimal(str(p)), Decimal(str(q))) for p, q in asks],
            "update_id": update_id,
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content=content, timestamp=time.time(), metadata=metadata)
    
    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["lastUpdateId"],
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: the changes in the order book
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to the difference data
        :return: a diff message with the changes in the order book notified by the exchange
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": int(msg['d']["r"]),
            "bids": [[i['p'], i['v']] for i in msg['d'].get("bids", [])],
            "asks": [[i['p'], i['v']] for i in msg['d'].get("asks", [])],
        }, timestamp=timestamp * 1e-3)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)
        ts = timestamp
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if msg["S"] == 2 else float(TradeType.BUY.value),
            "trade_id": msg["t"],
            "update_id": ts,
            "price": msg["p"],
            "amount": msg["v"]
        }, timestamp=ts * 1e-3)
