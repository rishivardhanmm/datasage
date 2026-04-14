import numbers

import matplotlib.pyplot as plt


def _is_chartable(results):
    if not results or len(results) < 2:
        return False

    first_row = results[0]
    if not isinstance(first_row, (list, tuple)) or len(first_row) != 2:
        return False

    label, value = first_row
    return not isinstance(label, numbers.Number) and isinstance(value, numbers.Number)


def plot_results(results, title="Query Result", show=False):
    if not _is_chartable(results):
        return None

    try:
        labels = [str(row[0]) for row in results]
        values = [float(row[1]) for row in results]

        fig, ax = plt.subplots(figsize=(8, 4.5))
        bars = ax.bar(labels, values, color="#1f6f8b")
        ax.set_xlabel("Category")
        ax.set_ylabel("Value")
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.2)

        for bar, value in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{value:g}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

        fig.tight_layout()

        if show:
            plt.show()

        return fig
    except Exception:
        return None

