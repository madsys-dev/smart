import common
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
from matplotlib import gridspec
import csv
from matplotlib.ticker import StrMethodFormatter, NullFormatter

threads = common.fig5_thread_set
zipfian = common.fig5_zipfian_set

# DO NOT CHANGE BELOW
def load_dataset(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(
                [float(data[7]), float(data[8]), float(data[9])]
            )
        return np.transpose(table).tolist()


def set_single_plt():
    fig = plt.figure(figsize=(8.5, 3.3))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 16.75
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def draw_graph(path):
    data = load_dataset(common.ae_data_path + "/fig5a.csv")
    fig = set_single_plt()
    ax1 = fig.add_subplot(121)
    ax1.set_title("(a)")
    Throughput = data[0]
    Latency50 = data[1]
    Latency99 = data[2]
    ax1.plot(
        threads,
        Throughput,
        marker="D",
        color="purple",
        label="Throughput",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax1.set_ylim([0, 2.0])
    ax1.set_ylabel("Throughput (MOP/s)")

    ax2 = ax1.twinx()
    ax2.plot(
        threads,
        Latency50,
        marker="s",
        color="red",
        label="Median Latency",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax2.plot(
        threads,
        Latency99,
        marker="<",
        color="brown",
        label="99th Latency",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )

    ax2.set_xlim([0, 16])
    ax2.set_xticks([0, 4, 8, 12, 16])
    ax1.set_xlabel("Thread Count")
    ax2.set_yscale("log")
    ax2.set_ylim([5, 10000])
    ax2.set_yticks([10, 100, 1000, 10000])
    ax2.set_ylabel("Latency ($\mu$s)")

    data = load_dataset(common.ae_data_path + "/fig5b.csv")
    Parameters = range(0, 6)
    Throughput = data[0]
    Latency50 = data[1]
    Latency99 = data[2]
    ax3 = fig.add_subplot(122)
    ax3.set_title("(b)")
    ax3.plot(
        Parameters,
        Throughput,
        marker="D",
        color="purple",
        label="Throughput",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax3.set_ylim([0, 6])
    ax3.set_ylabel("Throughput (MOP/s)")

    ax4 = ax3.twinx()
    ax4.plot(
        Parameters,
        Latency50,
        marker="s",
        color="red",
        label="Median Latency",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax4.plot(
        Parameters,
        Latency99,
        marker="<",
        color="brown",
        label="99th Latency",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax4.set_xlim([0, 5])
    ax4.set_xticks(Parameters, zipfian)
    ax3.set_xlabel("Zipfian Parameter")
    ax4.set_yscale("log")
    ax4.set_ylim([5, 10000])
    ax4.set_yticks([10, 100, 1000, 10000])
    ax4.set_ylabel("Latency ($\mu$s)")

    h1, l1 = ax2.get_legend_handles_labels()
    h2, l2 = ax1.get_legend_handles_labels()
    plt.legend(
        h1 + h2,
        l1 + l2,
        bbox_to_anchor=(0.1, 1.2, -0.8, 0.7),
        loc="lower left",
        mode="expand",
        borderaxespad=0,
        frameon=False,
        ncol=3,
        handletextpad=0.1,
        prop={"size": 16},
    )
    ax1.grid(linestyle="-.")
    ax3.grid(linestyle="-.")
    plt.tight_layout()
    plt.savefig(path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig5.pdf")
