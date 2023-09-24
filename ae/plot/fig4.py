import common
from turtle import position
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import numpy as np
from matplotlib import ticker
from matplotlib import gridspec
import csv

work_requests = common.fig4_depth_set
threads = common.fig4_thread_set


def load_dataset(dataset_path):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[5]))
        return np.reshape(table, [len(threads), len(work_requests)]).transpose()


def set_single_plt():
    fig = plt.figure(figsize=(9, 3.5))
    plt.rcParams["xtick.direction"] = "in"
    plt.rcParams["ytick.direction"] = "in"
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.serif"] = ["Arial"]
    plt.rcParams["mathtext.fontset"] = "cm"
    plt.rcParams["font.size"] = 18
    plt.rcParams["hatch.linewidth"] = 0.90
    return fig


def heatmap(
    y_label, data, row_labels, col_labels, ax=None, cbar_kw={}, cbarlabel="", **kwargs
):
    if not ax:
        ax = plt.gca()

    im = ax.imshow(data, **kwargs)
    # cbar = ax.figure.colorbar(im, ax=ax, shrink=0.75, pad=-0.025)
    # cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")
    # cbar.ax.tick_params(labelsize=12)
    ax.set_xticks(np.arange(data.shape[1]), labels=col_labels)
    ax.set_yticks(np.arange(data.shape[0]), labels=row_labels)
    ax.tick_params(top=True, bottom=False, labeltop=True, labelbottom=False)
    ax.set_xlabel("Thread Count")
    ax.xaxis.set_label_position("top")
    # ax.set_ylabel("Outstanding WRs\nPer Thread")
    # plt.setp(ax.get_xticklabels(), rotation=0, ha="right", rotation_mode="anchor")
    ax.spines[:].set_visible(False)
    ax.set_xticks(np.arange(data.shape[1] + 1), minor=True)
    ax.set_yticks(np.arange(data.shape[0] + 1), minor=True)
    ax.grid(which="minor", color="w", linestyle="-", linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)
    return im


def annotate_heatmap(
    im,
    data=None,
    valfmt="{x:.2f}",
    textcolors=("black", "white"),
    threshold=None,
    **textkw
):
    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max()) / 2.0

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center", verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = ticker.StrMethodFormatter(valfmt)

    valfmt2 = ticker.StrMethodFormatter("{x:.0f}")

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            if data[i, j] > 100.0:
                text = im.axes.text(j, i, valfmt2(data[i, j], None), **kw)
            else:
                text = im.axes.text(j, i, valfmt2(data[i, j], None), **kw)
            texts.append(text)

    return texts


def draw_graph(figure_path):
    fig = set_single_plt()
    ax = fig.add_subplot(121)
    ax.set_title("(a) 8-byte READ", size=18)
    throughput = load_dataset(common.ae_data_path + "/fig4a.csv")
    im = heatmap(True, throughput, work_requests, threads, ax=ax, cmap="YlGn")
    ax.set_ylabel("#OWRs/Thread")
    annotate_heatmap(im, valfmt="{x:.1f}", size=17)
    ax = fig.add_subplot(122)
    ax.set_title("(b) 8-byte WRITE", size=18)
    throughput = load_dataset(common.ae_data_path + "/fig4b.csv")
    im = heatmap(
        False,
        throughput,
        work_requests,
        threads,
        ax=ax,
        cmap="YlGn",
        cbarlabel="Throughput (MOPS)",
    )
    ax.set_ylabel("#OWRs/Thread")
    annotate_heatmap(im, valfmt="{x:.1f}", size=17)
    plt.tight_layout(pad=0)
    plt.savefig(figure_path, bbox_inches="tight", pad_inches=0)
    plt.close()


draw_graph(common.ae_figure_path + "/fig4.pdf")
