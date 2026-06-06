from order_generator import OrderGenerator, GeneratorConfig
from collections import Counter

config = GeneratorConfig()
config.aggressive_new_orders = 0.3
config.passive_new_orders = 0.3
config.cancel_orders = 0.2
config.modify_orders = 0.2

generator = OrderGenerator(1, config)
all_generated_orders = []
for i in range(100):
    all_generated_orders.append(generator.generate_next())
    print(all_generated_orders[-1])


stats = Counter()
for order in all_generated_orders:
    if order["action"] == "new_order":
        stats[order.get("type", "unknown")] += 1
    else:
        stats[order["action"]] += 1

print(f"\nGenerated {sum(stats.values())} orders:")
for op, count in stats.most_common():
    pct = count / sum(stats.values()) * 100
    print(f"  {op}: {count} ({pct:.1f}%)")
