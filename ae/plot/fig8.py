import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
import csv
from matplotlib.ticker import StrMethodFormatter, NullFormatter
from matplotlib import gridspec
import common

# Configuration
threads = common.thread_set


# DO NOT CHANGE BELOW
def load_dataset(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[7]))
        return np.reshape(table, [len(table) // len(threads), len(threads)])


def set_single_plt():
    fig = plt.figure(figsize=(8, 3))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 18
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def plot_figure(ax, data):
    ax.plot(
        threads,
        data[0],
        marker="D",
        color="purple",
        label="RACE",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        data[1],
        marker="s",
        color="red",
        label="O1: +ThdResAlloc",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        data[2],
        marker="<",
        color="brown",
        label="O2: O1+WorkReqThrot",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        data[3],
        marker="v",
        color="darkblue",
        label="O3: O2+ConflictAvoid",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.set_xlim([0, 100])
    ax.grid(linestyle="-.")


def draw_graph(path):
    fig = set_single_plt()
    gs = gridspec.GridSpec(1, 3, width_ratios=[2, 2, 2])
    ax = fig.add_subplot(gs[0])
    ax.set_title("(a) Write-heavy", size=18)
    data = load_dataset(common.ae_data_path + "/fig8-ycsb-a.csv")
    plot_figure(ax, data)
    ax.set_ylabel("Throughput (MOP/s)")
    ax.set_ylim([0, 8])
    ax.set_yticks([0, 4, 8])

    h1, l1 = ax.get_legend_handles_labels()
    plt.legend(
        h1,
        l1,
        bbox_to_anchor=(0.05, 1.20, 3.2, 0.7),
        loc="lower left",
        mode="expand",
        borderaxespad=0,
        frameon=False,
        ncol=2,
        handletextpad=0.3,
    )

    ax = fig.add_subplot(gs[1])
    ax.set_title("(b) Read-heavy", size=18)
    data = load_dataset(common.ae_data_path + "/fig8-ycsb-b.csv")
    plot_figure(ax, data)
    ax.set_xlabel("Thread Count")
    ax.set_ylim([0, 30])
    ax.set_yticks([0, 10, 20, 30])

    ax = fig.add_subplot(gs[2])
    ax.set_title("(c) Read-only", size=18)
    data = load_dataset(common.ae_data_path + "/fig8-ycsb-c.csv")
    plot_figure(ax, data)
    ax.set_ylim([0, 30])
    ax.set_yticks([0, 10, 20, 30])

    plt.tight_layout(pad=0)
    plt.savefig(path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig8.pdf")
