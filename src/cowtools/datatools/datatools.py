import gzip
import json
import pickle
import warnings

DEFAULT_GROUPING_MAP = {
    "QCD": lambda dset: dset.startswith("/QCD"),
    "ZJets": lambda dset: dset.startswith("/Zto"),
    "ttbar": lambda dset: dset.startswith("/TTto"),
    "SingleTop": lambda dset: dset.startswith("/TWminus")
    or dset.startswith("/TbarWplus"),
    "Diboson": lambda dset: dset.startswith("/WWto")
    or dset.startswith("/WZ")
    or dset.startswith("/ZZto"),
}


def combine_rename_results(in_hists, grouping_map=None, short_name_map=None):
    """
    Take a dictionary mapping dataset names to dictionaries of analysis results, and group certain datasets together
    under one name, while also replacing some dataset names with shorter versions.

    Note: Be careful that two functions in grouping_map cannot both be True for the same input dset name. If that
    happens, it is not guaranteed which grouped dataset dset's results will be a part of.

    Inputs:
        in_hists: (dict) Values are dicts containing things that can be added (hists, floats, etc.)
        grouping_map: (dict) Keys are grouped dset names, and values are functions with signature fn(dset) for
                      dset a str. If fn(dset) evaluates to True, then in_hists[dset]'s values are accumulated
                      together with other dsets passing fn in the output hist.
        short_name_map: (dict) Keys are same as keys from in_hists. If a dset that is a key in in_hists passes no
                        functions from grouping_map, but is a key in short_name_map, then short_name_map[dset]
                        is the new dset name in the output hist.
    """
    # Check that grouping_map's keys and short_name_map's values have no strings in common
    if short_name_map is None:
        short_name_map = {}
    if grouping_map is None:
        grouping_map = {}
    _names_in_both = set.intersection(
        set(grouping_map.keys()), set(short_name_map.values())
    )
    if _names_in_both != set():
        warnings.warn(
            f"""
                        Warning: grouping_map and short_name_map both indicate that they would like to create
                        datasets in the output dict with the following names: {_names_in_both}. This may lead
                        to some results overwriting others. This is only safe if every dset pointing to a shared
                        group name in short_name_map passes a function in grouping_map. Proceed with caution!"""
        )

    # Initialize output hist
    out_dict = {}

    for dset, results in in_hists.items():
        is_in_group = False  # control for whether or not dset should have its own (ungrouped) key in output
        for group_name, fn in grouping_map.items():
            if not fn(dset):
                continue
            # If we make it here, then fn(dset) is True
            if group_name not in out_dict:
                out_dict[group_name] = results
            else:
                for obs_name, obs in results.items():
                    try:
                        out_dict[group_name][obs_name] += obs
                    except KeyError:
                        raise ValueError(
                            """Two result histograms cannot be grouped together due to different
                                            structures. Please make sure that all values in in_hists have the
                                            same set of keys."""
                        )
            # At this point, we should not check other group_names, since we already accumulated the results
            is_in_group = True
            break
        if not is_in_group:
            # Only get here if fn(dset) was False for all fn
            short_name = short_name_map.get(
                dset, dset
            )  # If dset is not a key, the short_name is just dset
            out_dict[short_name] = results

    return out_dict


# General, just scale things by luminosity
def scale_results(
    mc, lumi, mc_xsecs, mc_evt_cnts, verbose=False, dont_scale=None
):
    """
    Inputs:
        mc: (dict) Values are dicts containing hists, floats, etc. to be scaled
        lumi: (float | int) Luminosity to scale to
        mc_xsecs: (dict) Map keys from mc to their cross sections
        mc_evt_cnts: (dict) Map keys from mc to their raw event counts
        dont_scale: (iterable, optional) An iterable of things to not scale. These are keys
            in the dicts that are themselves values of mc.

    Outputs:
        Hist with the same structure as mc, except results are scaled to lumi according to
        mc_xsecs and mc_evt_cnts
    """
    # If mc is a string, treat as filepath and retrieve results
    if dont_scale is None:
        dont_scale = ["RawEventCount"]
    if type(mc) == str:
        # If strings end in .pkl, use pickle to load
        if not mc.endswith(".pkl"):
            warnings.warn(
                f"""
                            Warning: arg mc is {mc}, and is type str, but does not end in '.pkl'. Trying to read with pickle
                            anyway. If this is not desired, please retrieve the hist of results and pass that as mc arg."""
            )
        with open(mc, "rb") as f:
            mc = pickle.load(f)

    out = {}
    for dset, results in mc.items():
        out[dset] = {}
        mc_factor = mc_xsecs[dset] * lumi / mc_evt_cnts[dset]
        if verbose:
            print(f"Dataset {dset} has MC lumi-scaling weight {mc_factor}")
        for obs_name, obs in results.items():
            if obs_name not in dont_scale:
                out[dset][obs_name] = obs * mc_factor
            else:
                out[dset][obs_name] = obs

    return out


class XSecScaler:
    def __init__(
        self,
        data,
        mc,
        fs_data,
        fs_mc,
        grouping_map_mc=DEFAULT_GROUPING_MAP,
        grouping_map_data=None,
    ):
        """
        This is an opinionated class. It essentially is scaffolding for cowtools.scale_results and
        cowtools.combine_rename_results. Those functions can be used to make an implementation that may be
        more flexible and/or suitable for different analysis frameworks.

        Take analysis results from data and MC samples and scale the MC results to the luminosity reported
        in the data results. Also, optionally rename datasets to shorter names and combine multiple datasets
        under common names. Beware that the units for cross sections and luminosities are assumed to be in
        pb and /pb.

        This class makes certain assumptions about the structure of the dictionaries
        passed to the constructor. These are:
          - For every key,value pair in data, value has a key "Luminosity"
          - For every key,value pair in fs_mc, value["metadata"]["metadata"]["xsec"] exists
          - For every key,value pair in MC, value has a key "RawEventCount"

        Inputs:
            data: (dict | str) Maps dataset names to dicts, which map strings to hists, floats, and other things
                  that can be added together. Satisfies the key assumptions listed above. If a str, must be an
                  accessible pickle file.
            mc: (dict | str) Maps dataset names to dicts, which map strings to hists, floats, and other things
                that can be added together. Satisfies the key assumptions listed above. If a str, must be an
                accessible pickle file.
            fs_data: (dict | str) A coffea input fileset. If short names are desired, input as
                     fs_data[dset]["metadata"]["short_name"]. If a str, must be gzipped, accessible, json file.
            fs_mc: (dict | str) A coffea input fileset. If short names are desired, input as
                   fs_mc[dset]["metadata"]["short_name"]. If a str, must be gzipped, accessible, json file.
            grouping_map_mc: (dict, optional) Map dataset group names group_name to functions with signature
                             fn(dset). If fn(dset) is True, then the results of dset will be grouped into a larger
                             dataset with name group_name. This is for the MC samples.
            grouping_map_data: (dict, optional) Map dataset group names group_name to functions with signature
                             fn(dset). If fn(dset) is True, then the results of dset will be grouped into a larger
                             dataset with name group_name. This is for the data samples.
        """
        # Load filesets, if they are strings (assuming they are paths to jsons)
        if grouping_map_data is None:
            grouping_map_data = {}
        if type(fs_data) == str:
            with gzip.open(fs_data, "rt") as f:
                fs_data = json.load(f)
        self.fs_data = fs_data
        if type(fs_mc) == str:
            with gzip.open(fs_mc, "rt") as f:
                fs_mc = json.load(f)
        self.fs_mc = fs_mc

        # Load results, if it is a string (assuming it is a path to a pkl)
        if type(data) == str:
            with open(data, "rb") as f:
                data = pickle.load(f)
        self.data = data
        if type(mc) == str:
            with open(mc, "rb") as f:
                mc = pickle.load(f)
        self.mc = mc

        self.grouping_map_mc = grouping_map_mc
        self.grouping_map_data = grouping_map_data

        # Default values that may be calculated later if requested
        self._scaled_mc = None
        self._scaled_combined_mc = None
        self._combined_data = None

        # Calculate luminosity
        self.lumi = 0
        for results in self.data.values():
            self.lumi += results["Luminosity"]

    @property
    def scaled_mc(self):
        """
        MC results, scaled according to luminosity obtained from data results.
        """
        if self._scaled_mc is None:
            self._scale_mc()
        return self._scaled_mc

    def _scale_mc(self):
        # Scale MC
        print(
            f"Scaling MC to luminosity {self.lumi/1000} fb^-1"
        )  # Convert assumed /pb to /fb
        mc_xsecs = {}
        mc_evt_cnts = {}
        for dset in self.mc:
            try:
                mc_xsecs[dset] = self.fs_mc[dset]["metadata"]["xsec"]
            except KeyError:
                mc_xsecs[dset] = self.fs_mc[dset]["metadata"]["metadata"]["xsec"]
            mc_evt_cnts[dset] = self.mc[dset]["RawEventCount"]
        self._scaled_mc = scale_results(self.mc, self.lumi, mc_xsecs, mc_evt_cnts)

    @property
    def scaled_combined_mc(self):
        """
        MC results, scaled to luminosity from the data results and then accumulated under group
        names provided by grouping_map_mc, or renamed according to short_names from fs_mc, if
        provided.
        """
        if self._scaled_combined_mc is None:
            self._scale_combine_mc()
        return self._scaled_combined_mc

    def _scale_combine_mc(self):
        # Combine MC datasets
        short_name_mc = {}
        for dset in self.scaled_mc:
            short_name_mc[dset] = self.fs_mc[dset]["metadata"].get("short_name", dset)
        self._scaled_combined_mc = combine_rename_results(
            self.scaled_mc,
            grouping_map=self.grouping_map_mc,
            short_name_map=short_name_mc,
        )

    @property
    def combined_data(self):
        """
        Data results, accumulated under group names provided by grouping_map_data, or renamed
        according to short_names from fs_data, if provided.
        """
        if self._combined_data is None:
            self._combine_data()
        return self._combined_data

    def _combine_data(self):
        # Combine data datasets
        short_name_data = {}
        for dset in self.data:
            short_name_data[dset] = self.fs_data[dset]["metadata"].get(
                "short_name", dset
            )
        self._combined_data = combine_rename_results(
            self.data,
            grouping_map=self.grouping_map_data,
            short_name_map=short_name_data,
        )
