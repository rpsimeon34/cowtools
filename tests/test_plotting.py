import hist
import matplotlib.pyplot as plt
import numpy as np
import pytest

import cowtools.plotting

"""
To test run:
pytest --mpl

When adding new tests, run:
pytest --mpl-generate-path=tests/baseline
"""

@pytest.fixture(scope="module")
def sample_hist1():
    """
    2D hist object with Gaussian data on each axis
    """
    #Make random number generator
    rng = np.random.default_rng(seed=42)
    #Generate random data with given mean, std, length
    data1 = rng.normal(0, 3, 1000)
    data2 = rng.normal(0, 5, 1000)

    #Make and fill 2D hist
    axis1 = hist.axis.Regular(18,-9,9,name="data1")
    axis2 = hist.axis.Regular(30,-15,15,name="data2")
    out = hist.Hist(axis1,axis2,name="Events")
    out.fill(data1=data1,data2=data2)

    return out

@pytest.fixture(scope="module")
def sample_hist2():
    """
    1D hist object with Gaussian data
    """
    #Make random number generator
    rng = np.random.default_rng(seed=43)
    #Generate random data with given mean, std, length
    data = rng.normal(2, 2, 500)

    #Make and fill hist
    axis = hist.axis.Regular(18,-9,9,name="data1")
    out = hist.Hist(axis,name="Events")
    out.fill(data1=data)

    return out

@pytest.fixture(scope="module")
def sample_hist3():
    """
    1D hist object with Gaussian data
    """
    #Make random number generator
    rng = np.random.default_rng(seed=44)
    #Generate random data with given mean, std, length
    data = rng.normal(-2, 2, 500)

    #Make and fill hist
    axis = hist.axis.Regular(18,-9,9,name="data1")
    out = hist.Hist(axis,name="Events")
    out.fill(data1=data)

    return out

@pytest.mark.mpl_image_compare(baseline_dir="baseline", filename="test_plot_1d.png")
def test_plot_1d(sample_hist1, sample_hist2, sample_hist3):
    fig, ax = plt.subplots(1,1, figsize=(10,10))
    ax = cowtools.plotting.plot_1d_ax(
        ax,
        "Sample Title",
        sgl_hists=[sample_hist1[:,sum]],
        sgl_label=["Signal"],
        bkg_hists=[sample_hist2, sample_hist3],
        bkg_label=["Sample Hist 2", "Sample Hist 3"],
        xlabel="Sample Data",
        year=2024,
        lumi=100.0
    )
    return fig

@pytest.mark.mpl_image_compare(baseline_dir="baseline", filename="test_plot_1d_sgl_stack.png")
def test_plot_1d_sgl_stack(sample_hist1, sample_hist2, sample_hist3):
    fig, ax = plt.subplots(1,1, figsize=(10,10))
    ax = cowtools.plotting.plot_1d_sgl_stack_ax(
        ax,
        "Sample Title",
        [sample_hist1[:,sum]],
        ["Signal"],
        [sample_hist2, sample_hist3],
        ["Sample Hist 2", "Sample Hist 3"],
        xlabel="Sample Data",
        year=2024,
        lumi=100.0
    )
    return fig

@pytest.mark.mpl_image_compare(baseline_dir="baseline", filename="test_plot_2d.png")
def test_plot_2d(sample_hist1):
    fig, ax = plt.subplots(1,1, figsize=(10,10))
    ax = cowtools.plotting.plot_2d_ax(
        ax,
        sample_hist1,
        title="Sample 2D Plot Title",
        xlabel="X-axis Label",
        ylabel="Y-axis Label",
        year=2025,
        lumi=150.0
    )
    return fig

@pytest.mark.mpl_image_compare(baseline_dir="baseline", filename="test_plot_2d_logz.png")
def test_plot_2d_logz(sample_hist1):
    fig, ax = plt.subplots(1,1, figsize=(10,10))
    ax = cowtools.plotting.plot_2d_ax(
        ax,
        sample_hist1,
        title="Sample 2D Plot Title",
        xlabel="X-axis Label",
        ylabel="Y-axis Label",
        year=2025,
        lumi=150.0,
        norm="log"
    )
    return fig