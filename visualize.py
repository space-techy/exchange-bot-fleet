# visualize_book.py
import matplotlib.pyplot as plt
from collections import defaultdict
from order_generator import OrderGenerator, GeneratorConfig

def build_and_plot(config, title):
    gen = OrderGenerator(bot_id=1, config=config)
    
    # Simulate: track resting orders by price level
    book_bids = defaultdict(int)  # price → total qty
    book_asks = defaultdict(int)
    
    for _ in range(config.total_orders):
        order = gen.generate_next()
        
        if order["action"] == "new_order":
            if order.get("price", 0) < gen.ref_price:
                # Passive buy — below mid
                book_bids[order["price"]] += order["qty"]
            else:
                # Passive sell — above mid
                book_asks[order["price"]] += order["qty"]
        elif order["action"] == "cancel":
            # Remove from whichever side it was on
            oid = order["order_id"]
            # (simplified — in reality you'd track which side each order is on)
    
    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    bid_prices = sorted(book_bids.keys())
    bid_qtys = [book_bids[p] for p in bid_prices]
    
    ask_prices = sorted(book_asks.keys())
    ask_qtys = [book_asks[p] for p in ask_prices]
    
    ax.bar(bid_prices, bid_qtys, color='green', alpha=0.7, label='Bids (buys)')
    ax.bar(ask_prices, ask_qtys, color='red', alpha=0.7, label='Asks (sells)')
    ax.axvline(x=config.fair_value, color='black', linestyle='--', label='Mid price')
    ax.set_xlabel('Price (ticks)')
    ax.set_ylabel('Total Quantity')
    ax.set_title(title)
    ax.legend()
    plt.tight_layout()
    plt.savefig(f"{title.replace(' ', '_').lower()}.png")
    plt.show()

# Compare different distributions
build_and_plot(
    GeneratorConfig(
        seed=42, total_orders=10000,
        passive_new_orders=1.0, aggressive_new_orders=0, cancel_orders=0, modify_orders=0,
        offset_lambda=0.2,
        start_price=1000, fair_value=1000
    ),
    "Exponential Offset (lambda=0.2)"
)

build_and_plot(
    GeneratorConfig(
        seed=42, total_orders=10000,
        passive_new_orders=1.0, aggressive_new_orders=0, cancel_orders=0, modify_orders=0,
        offset_lambda=0.05,
        start_price=1000, fair_value=1000
    ),
    "Exponential Offset (lambda=0.05) - Wide"
)