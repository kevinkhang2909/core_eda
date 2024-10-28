import numpy as np
from scipy.spatial.distance import jensenshannon


def bisect_search(arr: np.array, cuts: list, label_names: list):
    array_search = np.searchsorted(cuts, arr)
    array_len_limit = np.asarray([len(cuts) - 1] * arr.shape[0])
    array_final = np.vstack((array_search, array_len_limit))
    idx = np.min(array_final, axis=0)
    label = [label_names[i] for i in idx]
    return label


def interpret_jsd(jsd_value):
    """Interprets the Jensen-Shannon Divergence (JSD) value using numpy.searchsorted.
    Args:
    jsd_value: The JSD value to interpret.
    Returns:
    A string indicating the similarity between the distributions based on the JSD value.
    """
    thresholds = np.array([0, 0.05, 0.1, 0.3])
    interpretations = [
        'Very similar',
        'Slightly different',
        'Moderately different',
        'Substantially different'
    ]
    index = min(np.searchsorted(thresholds, jsd_value), len(interpretations) - 1)
    return interpretations[index]


def jsd(dist1, dist2, bins: int = 50):
    # Compute histograms
    hist1, bin_edges = np.histogram(dist1, bins=bins, density=True)
    hist2, _ = np.histogram(dist2, bins=bin_edges, density=True)

    # Add small constant to avoid division by zero
    hist1 = hist1 + 1e-10
    hist2 = hist2 + 1e-10

    # Normalize histograms
    hist1 = hist1 / np.sum(hist1)
    hist2 = hist2 / np.sum(hist2)
    js_div = jensenshannon(hist1, hist2).item()

    return {
        'score': js_div,
        'meaning': interpret_jsd(js_div)
    }