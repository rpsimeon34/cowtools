import hist.intervals
import hist.plot
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
from cycler import cycler

TABLEAU_COLORS = [
    "blue",
    "orange",
    "green",
    "red",
    "purple",
    "brown",
    "pink",
    "gray",
    "olive",
    "cyan",
]

hep.style.use(hep.style.CMS)


def plot_1d(*args, **kwargs):
    """
    See plot_1d_ax. Creats only one plot, and displays it to screen.

    Inputs
    ------
    *args:
        Arguments to pass to plot_1d_ax, except ax. Do not include an arg for ax.
    **kwargs:
        Keyword arguments to pass to plot_1d_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    ax = plot_1d_ax(ax, *args, **kwargs)

    plt.show()


def plot_1d_ax(
    ax,
    title,
    bkg_hists=None,
    bkg_label=None,
    sgl_hists=None,
    sgl_label=None,
    xlabel=None,
    xlim=None,
    signal_sf=1,
    logy=False,
    density=False,
    sort="yield",
    title_pos=None,
    year=None,
    lumi=None,
    com=13.6,
    legend_xoffset=0.02,
    flow="hint",
):
    """
    Make a 1D histogram plot with stacked backgrounds and signals as lines.
    Shows plots as "CMS Preliminary", puts a legend in the top right, and calls
    the y axis "Events".

    Inputs
    ------
    ax: matplotlib.pyplot axis
        The axis to put this plot on
    title: str
        The title of the plot.
    bkg_hists: iterable
        An iterable of single-axis histograms to be stacked
        in the plot. May need to sum/integrate over all but one axis
        to be iterable.
    bkg_label: iterable
        Iterable of strings of the same length as bkg_hists.
        This goes in the legend. The first label goes with the first
        histogram in bkg_hists, etc.
    sgl_hists: iterable
        Like bkg_hists, but will be overlayed as lines,
        instead of stacked.
    sgl_label: iterable
        Like bkg_label, but with the same length as sgl_hists.
    xlabel: str
        The label for the x-axis. If not given, no label put on plot.
    xlim: iterable of length 2
        Zeroth argument is lower x-axis limit, last argument is upper x-axis limit.
    signal_sf: float | int
        Scales the signal by this factor in plots and labels the key as so
    logy: bool
        If True, makes y-axis log-scaled
    density: bool
        If True, make all histograms plotted normed so their integrals
        are equal to 1.
    sort: str | None, optional
        The sort kwarg to be passed to mplhep's histplot function. If no
        sorting is desired, pass None.
    title_pos: float
        If set, sets the height of the title in matplotlib coordinates (1.0 is top),
    year: str | int
        If set, labels the plot with a year
    lumi: str | int
        If set, labels the plot with a luminosity
    com: float
        The center-of-mass energy to label the plot with, in TeV. Defaults to 13.6 TeV.
    flow: str
        How to show under/overflow bins. Passed to mplhep.histplot
    """

    if bkg_hists:
        hep.histplot(
            bkg_hists,
            ax=ax,
            stack=True,
            histtype="fill",
            label=bkg_label,
            density=density,
            sort=sort,
            flow=flow,
        )
    if sgl_hists:
        if signal_sf != 1:
            scaled_sgl_hists = [signal_sf * hist for hist in sgl_hists]
            scaled_sgl_label = [label + f" (x{signal_sf})" for label in sgl_label]
            hep.histplot(
                scaled_sgl_hists,
                ax=ax,
                label=scaled_sgl_label,
                density=density,
                sort=sort,
                flow=flow,
            )
        else:
            hep.histplot(
                sgl_hists, ax=ax, label=sgl_label, density=density, sort=sort, flow=flow
            )

    if lumi or year:
        hep.cms.label(
            lumi=lumi, year=year, loc=1, fontsize=14, lumi_format="{:.1f}", com=com
        )
    if (lumi or year) and not title_pos:
        ax.set_title(title, y=1.07, pad=2)
    else:
        ax.set_title(title, y=title_pos, pad=2)
    ax.legend(
        fontsize=10, loc="upper right", bbox_to_anchor=(1.0 + legend_xoffset, 0.999)
    ).shadow = True
    if density:
        ax.set_ylabel("A.U.", fontsize=10)
    else:
        ax.set_ylabel("Events", fontsize=10)
    ax.set_xlabel(xlabel, fontsize=10, labelpad=2)
    if xlim:
        ax.set_xlim(xlim[0], xlim[1])
    else:
        if ax.get_xlim()[0] < 0:
            ax.set_xlim(
                abs(ax.get_xlim()[0]) * -1.2,
                abs(ax.get_xlim()[1]) * 0.2 + ax.get_xlim()[1],
            )
        else:
            ax.set_xlim(0, ax.get_xlim()[1] * 1.2)
    if logy:
        ax.semilogy()
    return ax


def plot_1d_tofile(outfile, *args, **kwargs):
    """
    Same as plot_1d, except saves the plot to a file.

    Inputs
    ------
    *args:
        Arguments to pass to plot_1d_ax, except ax. Do not include an arg for ax.
    **kwargs:
        Keyword arguments to pass to plot_1d_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    ax = plot_1d_ax(ax, *args, **kwargs)

    plt.savefig(outfile, bbox_inches="tight")
    print(f"Saved output to {outfile}")
    plt.close()


def plot_1d_sgl_stack_ax(
    ax,
    title,
    sgl_hists,
    sgl_label,
    bkg_hists,
    bkg_label,
    xlabel=None,
    xlim=None,
    logy=False,
    density=False,
    sort="yield",
    title_pos=None,
    year=None,
    lumi=None,
    com=13.6,
    legend_xoffset=0.02,
    flow="hint",
):
    """
    Make a 1D histogram plot with signals stacked on top of (also stacked) backgrounds.
    Shows the background histograms as filled, and the signal ones and transparent.
    Shows plots as "CMS Preliminary", puts a legend in the top right, and calls
    the y axis "Events".

    Inputs
    ------
    ax: matplotlib.pyplot axis
        The axis to put this plot on
    title: str
        The title of the plot.
    sgl_hists: iterable
        Like bkg_hists, but will be overlayed as lines,
        instead of stacked. At most 10 total (background+signal) histograms
        can be plotted.
    sgl_label: iterable
        Like bkg_label, but with the same length as sgl_hists.
    bkg_hists: iterable
        An iterable of single-axis histograms to be stacked
        in the plot. May need to sum/integrate over all but one axis
        to be iterable. At most 10 total (background+signal) histograms
        can be plotted.
    bkg_label: iterable
        Iterable of strings of the same length as bkg_hists.
        This goes in the legend. The first label goes with the first
        histogram in bkg_hists, etc.
    xlabel: str
        The label for the x-axis. If not given, no label put on plot.
    xlim: iterable of length 2
        Zeroth argument is lower x-axis limit, last argument is upper x-axis limit.
    logy: bool
        If True, makes y-axis log-scaled
    density: bool
        If True, make all histograms plotted normed so their integrals
        are equal to 1.
    sort: str | None, optional
        The sort kwarg to be passed to mplhep's histplot function. If no
        sorting is desired, pass None.
    title_pos: float
        If set, sets the height of the title in matplotlib coordinates (1.0 is top),
    year: str | int
        If set, labels the plot with a year
    lumi: str | int
        If set, labels the plot with a luminosity
    com: float
        The center-of-mass energy to label the plot with, in TeV. Defaults to 13.6 TeV.
    flow: str
        How to show under/overflow bins. Passed to mplhep.histplot
    """
    # Get the colors for the plot
    if len(bkg_hists) + len(sgl_hists) > len(TABLEAU_COLORS):
        raise ValueError(
            "In plot_1d, at most 10 total (background+signal)"
            + f"histograms can be plotted at once. Tried to plot {len(bkg_hists)}"
            + f"background and {len(sgl_hists)} signal histograms"
        )
    fill_colors = TABLEAU_COLORS[: len(bkg_hists)] + ["none"] * len(sgl_hists)
    edge_colors = TABLEAU_COLORS[: len(bkg_hists) + len(sgl_hists)]
    print(f"fill colors are: {fill_colors}")
    print(f"edge colors are: {edge_colors}")

    hep.histplot(
        bkg_hists + sgl_hists,
        ax=ax,
        stack=True,
        histtype="fill",
        label=bkg_label + sgl_label,
        facecolor=fill_colors,
        edgecolor=edge_colors,
        linewidth=1,
        density=density,
    )

    if lumi or year:
        hep.cms.label(
            lumi=lumi, year=year, loc=1, fontsize=14, lumi_format="{:.1f}", com=com
        )
    if (lumi or year) and not title_pos:
        ax.set_title(title, y=1.07, pad=2)
    else:
        ax.set_title(title, y=title_pos, pad=2)
    ax.legend(
        fontsize=10, loc="upper right", bbox_to_anchor=(1.0 + legend_xoffset, 0.999)
    ).shadow = True
    if density:
        ax.set_ylabel("A.U.", fontsize=10)
    else:
        ax.set_ylabel("Events", fontsize=10)
    ax.set_xlabel(xlabel, fontsize=10, labelpad=2)
    if xlim:
        ax.set_xlim(xlim[0], xlim[1])
    else:
        if ax.get_xlim()[0] < 0:
            ax.set_xlim(
                abs(ax.get_xlim()[0]) * -1.2,
                abs(ax.get_xlim()[1]) * 0.2 + ax.get_xlim()[1],
            )
        else:
            ax.set_xlim(0, ax.get_xlim()[1] * 1.2)
    if logy:
        ax.semilogy()
    return ax


def plot_1d_sgl_stack(*args, **kwargs):
    """
    See plot_1d_sgl_stack_ax. Shows plot to screen

    Inputs
    ------
    *args:
        args to pass to plot_1d_sgl_stack_ax, except ax. Do not include an arg for ax.
    **kwargs:
        kwargs to pass to plot_1d_sgl_stack_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    ax = plot_1d_sgl_stack_ax(ax, *args, **kwargs)

    plt.show()


def plot_1d_sgl_stack_tofile(outfile, *args, **kwargs):
    """
    See plot_1d_sgl_stack. Saves plot to file

    Inputs
    ------
    *args:
        Arguments to pass to plot_1d_sgl_stack_ax, except ax. Do not include an arg for ax.
    **kwargs:
        Keyword arguments to pass to plot_1d_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    ax = plot_1d_sgl_stack_ax(ax, *args, **kwargs)

    plt.savefig(outfile, bbox_inches="tight")
    print(f"Saved output to {outfile}")


def plot_2d_ax(
    ax,
    hist2d,
    title=None,
    xlabel=None,
    xlim=None,
    logx=False,
    ylabel=None,
    ylim=None,
    logy=False,
    xbar_low=None,
    xbar_high=None,
    ybar_low=None,
    ybar_high=None,
    title_pos=None,
    year=None,
    lumi=None,
    com=13.6,
    norm=None,
    flow="hint",
):
    """
    Plot a 2D histogram with various customizations, including writing "CMS Preliminary", writing
    the COM energy, potentially the luminosity, year, and title.

    Inputs
    ------
    hist2d: Hist.hist
        The 2D histogram to plot
    title: str, optional
        The title of the plot.
    xlabel: str, optional
        The label for the x-axis. If not given, no label put on plot.
    xlim: iterable of length 2, optional
        Zeroth argument is lower x-axis limit last argument is upper x-axis limit.
    logx: bool, default False
        Show the x-axis on a log scale
    ylabel: str, optional
        The label for the y-axis
    ylim: iterable of length 2, optional
        Zeroth argument is lower y-axis limit last argument is upper y-axis limit.
    logy: bool, optional
        If True, makes y-axis log-scaled
    xbar_low: float, optional
        Draw a vertical line at this x-value
    xbar_high: float, optional
        Draw a vertical line at this x-value
    ybar_low: float, optional
        Draw a horizontal line at this y-value
    ybar_high: float, optional
        Draw a horizontal line at this y-value
    title_pos: float, optional
        If set, sets the height of the title in matplotlib coordinates (1.0 is top)
    year: str | int, optional
        If set, labels the plot with a year
    lumi: str | int, optional
        If set, labels the plot with a luminosity
    com: float, optional
        The center of mass energy, in GeV
    norm: str, optional
        If given, sets the normalization of the z-axis (color) eg: log
    flow: bool | str, default "hint"
        If True, show underflow and overflow bins
    """
    hep.hist2dplot(hist2d, ax=ax, norm=norm, flow=flow)
    if lumi or year:
        hep.cms.label(
            lumi=lumi, year=year, loc=1, fontsize=14, lumi_format="{:.1f}", com=com
        )
    if (lumi or year) and not title_pos:
        ax.set_title(title, y=1.07, pad=2)
    else:
        ax.set_title(title, y=title_pos, pad=2)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_xlabel(xlabel, fontsize=10, labelpad=2)
    if xlim:
        ax.set_xlim(xlim[0], xlim[1])
    if logx:
        ax.semilogx()
    if ylim:
        ax.set_ylim(ylim[0], ylim[1])
    if logy:
        ax.semilogy()
    if xbar_low:
        ax.plot((xbar_low, xbar_low), (ax.get_ylim()[0], ax.get_ylim()[1]), color="red")
    if xbar_high:
        ax.plot(
            (xbar_high, xbar_high), (ax.get_ylim()[0], ax.get_ylim()[1]), color="red"
        )
    if ybar_low:
        ax.plot((ax.get_xlim()[0], ax.get_xlim()[1]), (ybar_low, ybar_low), color="red")
    if ybar_high:
        ax.plot(
            (ax.get_xlim()[0], ax.get_xlim()[1]), (ybar_high, ybar_high), color="red"
        )
    return ax


def plot_2d(*args, **kwargs):
    """
    See plot_2d_ax. Shows plot to screen

    Inputs
    ------
    *args:
        args to pass to plot_2d_ax, except ax. Do not include an arg for ax.
    **kwargs:
        kwargs to pass to plot_2d_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    plot_2d_ax(ax, *args, **kwargs)

    plt.show()


def plot_2d_tofile(outfile, *args, **kwargs):
    """
    See plot_2d_ax. Saves plot to file

    Inputs
    ------
    *args:
        args to pass to plot_2d_ax, except ax. Do not include an arg for ax.
    **kwargs:
        kwargs to pass to plot_2d_ax
    """
    fig, ax = plt.subplots(1, 1, figsize=(7, 4.7))

    ax = plot_2d_ax(ax, *args, **kwargs)

    plt.savefig(outfile, bbox_inches="tight")
    print(f"Saved output to {outfile}")


def plot_wRatio(
    hMCs,
    hData,
    MC_labels,
    title,
    sgl_hists=None,
    sgl_label=None,
    signal_sf=1,
    lumi=100.0,
    year=2023,
    com=13.6,
    title_pos=1.07,
    legend_xoffset=0.02,
    xlabel=None,
    xlim=None,
    logy=False,
    outfile=None,
):
    """
    Plot histograms as in plot_1d_ax, but also with data overlayed as scatter points. Also show a second pane underneath
    with the data/MC-ratio. Includes statistical uncertainties on both MC and data. Optionally also plots the signal MC
    distribution overlayed on the top pane as a step-type (unfilled bar) histogram.

    Note that hMCs have to use hist.storage.Weight() as storage, otherwise
    we cannot add them together and get variances.

    Inputs
    ------
    hMCs: iterable[hist.Hists]
        An iterable of 1D histograms to plot, representing MC results. Histograms in this iterable must all have
        the same binning. Must use hist.storage.Weight() as storage, otherwise we cannot add them together and
        get variances/uncertainties.
    hData: hist.Hist
        A single histogram representing the data
    MC_labels: iterable[str]
        An iterable of strings with the labels for the MC histograms. Must be in the same order as the histograms
        in hMCs.
    title: str
        Title of the plot
    sgl_hists: iterable[hist.Hists], optional
        Signal MC histograms to be overlayed on the plot
    sgl_label: iterable[str], optional
        Labels for the signal MC histograms in sgl_hists. Must be in the same order as the histograms in sgl_hists.
    signal_sf: float, default 1
        Scale sgl_hists on the plot by this factor. Useful for making sure sgl_hists are visible.
    lumi: float, default 100.0
        The luminosity of information shown in the plot
    year: int, default 2023
        The year that information in the plot is from
    com: float, default 13.6
        The center of mass energy, in TeV
    title_pos: float, default 1.07
        Sets the height of the title in matplotlib coordinates (1.0 is top)
    legend_xoffset: float, default 0.02
        The offset applied to the legend along the x-axis
    xlabel: str, optional
        Label for the x-axis
    xlim: iterable[float] of length 2, optional
        Range of the x-axis to display. First element is the lower bound. If not given, limits will be inferred
        using the provided histograms.
    logy: bool, default False
        If True, show the y-axis on a log scale
    outfile: str, optional
        If given, saves the plot to a file. Otherwise, displays to screen
    """
    # If more than 6 things plotted, use 10-color palette
    if len(hMCs) > 6:
        colors = [
            "#3f90da",
            "#ffa90e",
            "#bd1f01",
            "#94a4a2",
            "#832db6",
            "#a96b59",
            "#e76300",
            "#b9ac70",
            "#717581",
            "#92dadd",
        ]
        hep.styles.CMS["axes.prop_cycle"] = cycler("color", colors)
        hep.style.use(hep.style.CMS)

    fig, (ax, rax) = plt.subplots(
        2, 1, figsize=(7, 7), gridspec_kw={"height_ratios": (3, 1)}, sharex=True
    )
    fig.subplots_adjust(hspace=0.07)

    hep.histplot(
        hMCs, ax=ax, stack=True, histtype="fill", label=MC_labels, sort="yield"
    )
    if sgl_hists:
        if signal_sf != 1:
            scaled_sgl_hists = [signal_sf * hist for hist in sgl_hists]
            scaled_sgl_label = [label + f" (x{signal_sf})" for label in sgl_label]
            hep.histplot(scaled_sgl_hists, ax=ax, label=scaled_sgl_label, sort="yield")
        else:
            hep.histplot(sgl_hists, ax=ax, label=sgl_label, sort="yield")
    ax.set_xlabel(None)

    mc_sum = sum(hMCs)

    mcStatUp = np.append(mc_sum.values() + np.sqrt(mc_sum.variances()), [0])
    mcStatDo = np.append(mc_sum.values() - np.sqrt(mc_sum.variances()), [0])

    ax.fill_between(
        hData.axes[0].edges,
        mcStatUp,
        mcStatDo,
        step="post",
        hatch="///",
        facecolor="none",
        edgecolor="gray",
        linewidth=0,
    )

    ax.errorbar(
        x=hData.axes[0].centers,
        y=hData.values(),
        yerr=np.sqrt(hData.values()),
        color="black",
        marker=".",
        markersize=10,
        linewidth=0,
        elinewidth=1,
        label="Data",
    )

    # Ratio plot
    ratio_mcStatUp = np.append(1 + np.sqrt(mc_sum.variances()) / mc_sum.values(), [0])
    ratio_mcStatDo = np.append(1 - np.sqrt(mc_sum.variances()) / mc_sum.values(), [0])

    rax.fill_between(
        hData.axes[0].edges,
        ratio_mcStatUp,
        ratio_mcStatDo,
        step="post",
        color="lightgray",
    )

    hist_1_values, hist_2_values = hData.values(), mc_sum.values()

    ratios = hist_1_values / hist_2_values
    ratio_uncert = hist.intervals.ratio_uncertainty(
        num=hist_1_values,
        denom=hist_2_values,
        uncertainty_type="poisson",
    )
    # ratio: plot the ratios using Matplotlib errorbar or bar
    hist.plot.plot_ratio_array(
        hData,
        ratios,
        ratio_uncert,
        ax=rax,
        uncert_draw_type="line",
    )
    # hData is just used for its bins in the above line

    if (lumi is not None) or (year is not None):
        hep.cms.label(
            ax=ax,
            lumi=lumi,
            year=year,
            loc=1,
            fontsize=12,
            lumi_format="{:.1f}",
            com=com,
        )
    if ((lumi is not None) or (year is not None)) and (title_pos is None):
        ax.set_title(title, y=1.07, pad=2, fontsize=14)
    else:
        ax.set_title(title, y=title_pos, pad=2, fontsize=14)
    ax.legend(
        fontsize=10, loc="upper right", bbox_to_anchor=(1.0 + legend_xoffset, 0.999)
    ).shadow = True
    ax.set_ylabel("Events", fontsize=10)
    if xlabel is not None:
        rax.set_xlabel(xlabel, fontsize=10, labelpad=2)
    if xlim is not None:
        ax.set_xlim(xlim[0], xlim[1])
    else:
        ax.set_xlim(0, ax.get_xlim()[1] * 1.2)
    if logy:
        ax.semilogy()

    hep.styles.CMS["axes.prop_cycle"] = cycler("color", hep.style.cms.cmap_petroff)
    hep.style.use(hep.style.CMS)

    if outfile is None:
        plt.show()
    else:
        plt.savefig(outfile)
        print(f"Saved output to {outfile}")
