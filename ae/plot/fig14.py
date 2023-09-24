from tkinter import Label
import matplotlib.pyplot as plt 
from matplotlib.pyplot import MultipleLocator
import numpy as np
import csv
from matplotlib.ticker import StrMethodFormatter, NullFormatter
import common

# Configuration
threads = common.fig14_thread_set


# DO NOT CHANGE BELOW
def load_dataset(dataset_path, idx):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        table = []
        for data in data_reader:
            table.append(float(data[idx]))
        return np.reshape(table, [len(table) // len(threads), len(threads)])


def load_distribution(dataset_path, line):
    with open(dataset_path) as dataset:
        data_reader = csv.reader(dataset, delimiter=",")
        idx = 0
        for data in data_reader:
            idx += 1
            if idx == line:
                table = []
                for j in range(9):
                    table.append(float(data[12 + j * 2]) * 100.0)
                print(table)
                return table


def set_single_plt():
    fig = plt.figure(figsize=(8.5, 2.4))
    plt.rcParams['xtick.direction'] = 'in'
    plt.rcParams['ytick.direction'] = 'in'
    plt.rcParams['axes.axisbelow'] = True
    plt.rcParams['font.family'] = 'serif'
    # plt.rcParams['font.serif'] = ['Arial']
    plt.rcParams['mathtext.fontset'] = 'cm'
    plt.rcParams['font.size'] = 18
    plt.rcParams['hatch.linewidth'] = 0.90
    return fig

def draw_graph(path):
    fig = set_single_plt()

    ax = fig.add_subplot(131)
    ax.set_title("(a)")
    data = load_dataset(common.ae_data_path + "/fig14.csv", 7)
    ax.plot(threads, data[0], marker='v', color="darkblue", label="Without\nConflictAvoid", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[1], marker='D', color="purple", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[2], marker='s', color="red", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[3], marker='<', color="brown", linewidth=1, ms=9, markerfacecolor='white')
    ax.set_xlim([0, 100])
    ax.grid(linestyle='-.')
    ax.set_xlabel("Thread Count")
    ax.set_ylabel("Throughput (MOP/s)")
    ax.set_ylim([0, 4.7])
    ax.set_yticks([0, 1, 2, 3, 4])
    ax.legend(borderaxespad=0, frameon=False, handletextpad=0.3, prop={'size': 17 })

    ax = fig.add_subplot(132)
    ax.set_title("(b)")
    data = load_dataset(common.ae_data_path + "/fig14bc.csv", 10)
    ax.plot(threads, data[0], marker='v', color="darkblue", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[1], marker='D', color="purple", label="+Backoff", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[2], marker='s', color="red",    label="+DynLimit", linewidth=1, ms=9, markerfacecolor='white')
    ax.plot(threads, data[3], marker='<', color="brown",    label="+CoroThrot", linewidth=1, ms=9, markerfacecolor='white')
    ax.set_xlim([0, 100])
    ax.grid(linestyle='-.')
    ax.set_xlabel("Thread Count")
    ax.set_ylabel("Average Retries")
    ax.set_ylim([0, 20])
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.legend(borderaxespad=0, frameon=False, handletextpad=0.3, prop={'size': 17 }, labelspacing=0.25)

    ax = fig.add_subplot(133)
    ax.set_title("(c)")
    X = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
    Label = ['1', '', '3', '', '5', '', '7', '', '$\\geq$9']
    width = 0.4
    dist = load_distribution(common.ae_data_path + "/fig14bc.csv", 32)
    ax.bar(X - width / 2, dist, width, color="skyblue", label="With\nConflictAvoid", alpha=.99, edgecolor='black', linewidth=0.5, hatch="////")
    dist = load_distribution(common.ae_data_path + "/fig14bc.csv", 8)
    ax.bar(X + width / 2, dist, width, color="yellow", label="Without\nConflictAvoid", alpha=.99, edgecolor='black', linewidth=0.5, hatch="....")
    ax.set_xlim([0, 2.5])
    ax.set_xticks(X, Label)
    ax.grid(linestyle='-.')
    ax.set_xlim([0, 10])
    ax.set_xlabel("Retries Per Op")
    ax.set_ylabel("Percentage")
    ax.set_ylim([0, 100])
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.legend(borderaxespad=0, frameon=False, handletextpad=0.3, prop={'size': 16 })

    plt.tight_layout(pad=-0.5)
    plt.savefig(path, bbox_inches='tight', pad_inches=0)
    plt.close()

draw_graph(common.ae_figure_path + "/fig14.pdf")
