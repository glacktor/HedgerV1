---
config:
  theme: redux
---
flowchart TD
 subgraph BinanceClient["BinanceClient"]
        BC1["connect_ws()"]
        BC2["place_order()"]
        BC3["subscribe_orderbook()"]
        BC4["subscribe_user_info()"]
  end
 subgraph HyperliquidClient["HyperliquidClient"]
        HC1["connect_ws()"]
        HC2["place_order()"]
        HC3["subscribe_orderbook()"]
        HC4["subscribe_user_info()"]
  end
 subgraph LighterClient["LighterClient"]
        LC1["connect_ws()"]
        LC2["place_order()"]
        LC3["subscribe_orderbook()"]
        LC4["subscribe_user_info()"]
  end
 subgraph ExtendedClient["ExtendedClient"]
        EC1["connect_ws()"]
        EC2["place_order()"]
        EC3["subscribe_orderbook()"]
        EC4["subscribe_user_info()"]
  end
 subgraph BinanceInfoClient["BinanceInfoClient"]
        BI1["get_orderbook()"]
        BI2["get_order_status()"]
        BI3["get_all_orders()"]
        BI4["delete_order()"]
  end
 subgraph HyperliquidInfoClient["HyperliquidInfoClient"]
        HI1["get_orderbook()"]
        HI2["get_order_status()"]
        HI3["get_all_orders()"]
        HI4["delete_order()"]
  end
 subgraph LighterInfoClient["LighterInfoClient"]
        LI1["get_orderbook()"]
        LI2["get_order_status()"]
        LI3["get_all_orders()"]
        LI4["delete_order()"]
  end
 subgraph ExtendedInfoClient["ExtendedInfoClient"]
        EI1["get_orderbook()"]
        EI2["get_order_status()"]
        EI3["get_all_orders()"]
        EI4["delete_order()"]
  end
 subgraph Tables1["Exchange1 Tables"]
        T1["orderbook1"]
        T2["userInfo1"]
  end
 subgraph Tables2["Exchange2 Tables"]
        T3["orderbook2"]
        T4["userInfo2"]
  end
 subgraph Tables3["Exchange3 Tables"]
        T5["orderbook3"]
        T6["userInfo3"]
  end
 subgraph Tables4["Exchange4 Tables"]
        T7["orderbook4"]
        T8["userInfo4"]
  end
 subgraph Database["In-Memory Database"]
    direction TB
        Tables1
        Tables2
        Tables3
        Tables4
  end
    A(["Start of alghorithm<br>"]) --> n1["1)Trading Algorithm Requirements Proxy Layer<br>2)Multi-Exchange Orderbook WebSocket Subscription<br>3)Account State WebSocket Subscription (Order Execution/Fill)"]
    n1 --> MainScript["Trading Algorithm Script (Core)"]
    MainScript --> ProxyLayer["Trading Algorithm Proxy Layer"] & BI1 & HI1 & LI1 & EI1
    ProxyLayer --> BC1 & HC1 & LC1 & EC1
    BC3 --> ZMQ["ZMQ Transport Protocol"]
    BC4 --> ZMQ
    HC3 --> ZMQ
    HC4 --> ZMQ
    LC3 --> ZMQ
    LC4 --> ZMQ
    EC3 --> ZMQ
    EC4 --> ZMQ
    ZMQ --> T1 & T2 & T3 & T4 & T5 & T6 & T7 & T8
    T1 --> BI1
    T2 --> BI2
    T3 --> HI1
    T4 --> HI2
    T5 --> LI1
    T6 --> LI2
    T7 --> EI1
    T8 --> EI2
    n1@{ shape: hex}
