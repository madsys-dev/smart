import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
import csv
from matplotlib.ticker import StrMethodFormatter, NullFormatter
import common

threads = common.thread_set
depth = common.fig13_depth_set


def load_dataset(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[5]))
        return np.reshape(table, [len(table) // len(threads), len(threads)])


def load_dataset_b(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[5]))
        return np.reshape(table, [len(table) // len(depth), len(depth)])


def set_single_plt():
    fig = plt.figure(figsize=(8, 3))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 16
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def plot_figure(ax, param, data):
    ax.plot(
        param,
        data[0],
        marker="D",
        color="purple",
        label="Per-thread QP",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        param,
        data[1],
        marker="<",
        color="brown",
        label="+ThdResAlloc",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        param,
        data[2],
        marker="v",
        color="darkblue",
        label="+WorkReqThrot",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.grid(linestyle="-.")


def draw_graph(path):
    fig = set_single_plt()
    ax = fig.add_subplot(121)
    ax.set_title("(a)")
    data = load_dataset(common.ae_data_path + "/fig13a.csv")
    plot_figure(ax, threads, data)
    ax.set_xlim([0, 100])
    ax.set_xlabel("Thread Count")
    ax.set_ylabel("Throughput (MOP/s)")
    ax.set_ylim([0, 150])
    ax.set_yticks([0, 50, 100, 150])

    h1, l1 = ax.get_legend_handles_labels()
    plt.legend(
        h1,
        l1,
        bbox_to_anchor=(0.0, 1.20, 2.2, 0.7),
        loc="lower left",
        mode="expand",
        borderaxespad=0,
        frameon=False,
        ncol=4,
        handletextpad=0.3,
        prop={"size": 16},
    )

    ax = fig.add_subplot(122)
    ax.set_title("(b)")
    data = load_dataset_b(common.ae_data_path + "/fig13b.csv")
    plot_figure(ax, depth, data)
    ax.set_xlim([0, 33])
    ax.set_xticks([0, 8, 16, 24, 32])
    ax.set_xlabel("Work Request Batch Size")
    ax.set_ylim([0, 150])
    ax.set_yticks([0, 50, 100, 150])

    plt.tight_layout()
    plt.savefig(path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig13.pdf")
