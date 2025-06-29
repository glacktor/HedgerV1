The aim of the project is to build a stable, manageable trading system that can be used for most speculative strategies in traditional financial markets. 

The separation of trading methods and the typification of the data returned from exchanges allow for easy integration of new trading platforms or modification/adding of new trading strategies. Additionally, the centralized database with connected sockets acts as a mirror of the exchanges' databases, ensuring minimal possible latency. This solution enables efficient data collection and storage, regardless of the scale.
Note: This Python implementation serves as a foundation for architecture validation. For high-frequency trading scenarios, the system is designed to be easily portable to C++/Rust for optimal performance.

architecture.png == architecture.md
