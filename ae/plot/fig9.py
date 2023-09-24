import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
import csv
from matplotlib.ticker import StrMethodFormatter, NullFormatter
import common

# DO NOT CHANGE BELOW
def load_dataset(dataset_path, type):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            if type == 0:
                table.append(
                    [float(data[7]), float(data[8])]
                )
            else:
                table.append(
                    [float(data[7]), float(data[9])]
                )
        return np.transpose(table).tolist()


def set_single_plt():
    fig = plt.figure(figsize=(8.0, 2.15))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 17
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def plot_figure2(ax, data):
    half = len(data[0]) // 2
    ax.plot(
        data[0][:half],
        data[1][:half],
        marker="D",
        color="purple",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        data[0][half:],
        data[1][half:],
        marker="v",
        color="darkblue",
        label="SMART-HT",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.grid(linestyle="-.")


def plot_figure(ax, data):
    half = len(data[0]) // 2
    ax.plot(
        data[0][:half],
        data[1][:half],
        marker="D",
        color="purple",
        label="RACE",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        data[0][half:],
        data[1][half:],
        marker="v",
        color="darkblue",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.grid(linestyle="-.")


def draw_graph(path):
    fig = set_single_plt()
    ax = fig.add_subplot(121)
    data = load_dataset(common.ae_data_path + "/fig9.csv", 0)
    plot_figure(ax, data)
    ax.set_ylabel("Median Latency\n($\mu$s)")
    ax.set_xlabel("Throughput (MOP/s)")
    ax.set_ylim([0, 120])
    ax.set_xlim([0, 16])
    ax.set_xticks([0, 4, 8, 12, 16])
    ax.set_yticks([0, 30, 60, 90, 120])
    h1, l1 = ax.get_legend_handles_labels()
    plt.legend(h1, l1, borderaxespad=0, frameon=False, ncol=1, handletextpad=0.3)

    ax = fig.add_subplot(122)
    data = load_dataset(common.ae_data_path + "/fig9.csv", 1)
    plot_figure2(ax, data)
    ax.set_ylabel("Tail Latency\n($\mu$s)")
    ax.set_xlabel("Throughput (MOP/s)")
    ax.set_ylim([0, 400])
    ax.set_xlim([0, 16])
    ax.set_xticks([0, 4, 8, 12, 16])
    ax.set_yticks([0, 100, 200, 300, 400])
    h1, l1 = ax.get_legend_handles_labels()
    plt.legend(h1, l1, borderaxespad=0, frameon=False, ncol=1, handletextpad=0.3)

    plt.tight_layout(pad=0)
    plt.savefig(path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig9.pdf")
