import json
import pandas as pd
from pathlib import Path

print(""""
        Flatten OTL navigatiestructuur naar een platgeslagen Excel-file
      """)

def load_navigatiestructuur(filepath: Path) -> dict:
    """Load OTL navigatiestructuur as JSON"""
    if not Path(filepath).exists():
        raise ValueError(f'Filepath {filepath} does not exist.')
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


# Recursive function to flatten the tree
def flatten_tree(node, depth=1, parent_path=None):
    """
    Recursively flattens a nested tree-like structure into a list of flat dicts.
    Each key in the output will be prefixed with the depth level.

    Args:
        node (dict): The current node of the tree.
        depth (int): The current depth in the tree.
        parent_path (list): List to track the path from the root to the current node.

    Returns:
        List[dict]: Flattened structure with depth-prefixed keys.
    """
    if parent_path is None:
        parent_path = []

    flat_row = {}
    prefix = f"{depth}_"

    for key, value in node.items():
        if key != "children":
            flat_row[prefix + key] = value

    full_path = parent_path + [flat_row]

    if "children" in node and node["children"]:
        flattened = []
        for child in node["children"]:
            flattened.extend(flatten_tree(child, depth + 1, full_path))
        return flattened
    else:
        # Merge the path into a single flat row
        final_row = {}
        for entry in full_path:
            final_row.update(entry)
        return [final_row]


if __name__ == '__main__':
    filepath = 'OTL_Navigatiestructuur.txt'
    otl_navigatiestructuur = load_navigatiestructuur(filepath=filepath)

    # Flatten the data
    flat_data = flatten_tree(otl_navigatiestructuur)

    # Convert to DataFrame
    df = pd.DataFrame(flat_data)
    df.sort_values(by=["1_name", "2_name", "3_name", "4_name", "5_name", "6_name", "7_name"])

    # Write to Excel
    df.to_excel("flat_otl_navigatiestructuur.xlsx", index=False, freeze_panes=[1,1])