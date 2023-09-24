import common
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
from matplotlib import gridspec
import csv

threads = common.thread_set

def load_dataset(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[5]))
        return np.reshape(table, [len(table) // len(threads), len(threads)])


def set_single_plt():
    fig = plt.figure(figsize=(8, 3.5))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 17
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def plot_figure(ax, dataset):
    shared_qp = dataset[0]
    multiplexed_qp_4 = dataset[1]
    multiplexed_qp_2 = dataset[2]
    per_thread_qp = dataset[3]
    per_thread_doorbell = dataset[4]
    ax.plot(
        threads,
        shared_qp,
        marker="^",
        color="orange",
        label="Shared QP",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        per_thread_qp,
        marker="D",
        color="black",
        label="Per-thread QP",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        multiplexed_qp_2,
        marker="s",
        color="red",
        label="Multiplexed QP ($q=2$)",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        multiplexed_qp_4,
        marker="<",
        color="brown",
        label="Multiplexed QP ($q=4$)",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.plot(
        threads,
        per_thread_doorbell,
        marker=">",
        color="purple",
        label="Per-thread Doorbell",
        linewidth=1,
        ms=8,
        markerfacecolor="white",
    )
    ax.grid(linestyle="-.")


def draw_graph(figure_path):
    fig = set_single_plt()

    ax = fig.add_subplot(121)
    ax.set_title("(a) 8-byte READ", size=18)
    data = load_dataset(common.ae_data_path + "/fig3a.csv")
    plot_figure(ax, data)
    ax.set_xlabel("Thread Count")
    ax.set_xlim([0, 100])
    ax.set_ylabel("Throughput (MOP/s)")
    ax.set_ylim([0, 120])
    ax.set_yticks([0, 40, 80, 120])
    plt.legend(
        bbox_to_anchor=(-0.2, 1.15, 2.55, 0.7),
        loc="lower left",
        mode="expand",
        borderaxespad=0,
        frameon=False,
        ncol=3,
        handletextpad=0.1,
        prop={"size": 16},
    )

    ax = fig.add_subplot(122)
    ax.set_title("(b) 8-byte WRITE", size=18)
    data = load_dataset(common.ae_data_path + "/fig3b.csv")
    plot_figure(ax, data)
    ax.set_xlabel("Thread Count")
    ax.set_xlim([0, 100])
    ax.set_ylim([0, 100])
    ax.set_yticks([0, 50, 100])

    plt.tight_layout()
    plt.savefig(figure_path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig3.pdf")
