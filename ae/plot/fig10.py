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
            table.append(float(data[4]))
        return np.reshape(table, [len(table) // len(threads), len(threads)])


def set_single_plt():
    fig = plt.figure(figsize=(7.5, 2.4))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 17
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def draw_graph(path):
    fig = set_single_plt()
    gs = gridspec.GridSpec(1, 2, width_ratios=[3, 3])
    ax = fig.add_subplot(gs[0])
    ax.set_title("(a) SmallBank")
    data = load_dataset(common.ae_data_path + "/fig10-smallbank.csv")
    ax.plot(
        threads,
        data[1],
        marker="v",
        color="darkblue",
        label="SMART-DTX",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        data[0],
        marker="D",
        color="purple",
        label="FORD+",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.set_xlabel("Thread Count")
    ax.set_xlim([0, 100])
    ax.set_ylabel("Throughput (MOP/s)")
    ax.set_ylim([0, 6])
    ax.set_yticks([0, 2, 4, 6])
    ax.grid(linestyle="-.")
    h1, l1 = ax.get_legend_handles_labels()
    plt.legend(
        h1,
        l1,
        borderaxespad=0,
        frameon=False,
        ncol=1,
        handletextpad=0.3,
        prop={"size": 17},
    )

    ax = fig.add_subplot(gs[1])
    ax.set_title("(b) TATP")
    data = load_dataset(common.ae_data_path + "/fig10-tatp.csv")
    ax.plot(
        threads,
        data[1],
        marker="v",
        color="darkblue",
        label="SMART-DTX",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        data[0],
        marker="D",
        color="purple",
        label="FORD+",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.set_xlabel("Thread Count")
    ax.set_xlim([0, 100])
    ax.set_ylim([0, 8])
    ax.set_yticks([0, 4, 8])
    ax.grid(linestyle="-.")

    plt.tight_layout(pad=0)
    plt.savefig(path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig10.pdf")
