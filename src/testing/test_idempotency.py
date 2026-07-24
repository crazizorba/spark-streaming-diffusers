#!/usr/bin/env python3
"""
Idempotency Test Script for Sang's CPG Parser
Verifies that parsing the same file multiple times produces identical Node IDs and Edge IDs.
"""

import os
import sys
from cpg_parser import parse_python_file


def test_idempotency():
    target_file = "diffusers/src/diffusers/__init__.py"
    if not os.path.exists(target_file):
        print(f"Target file {target_file} not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Testing Idempotency on: {target_file}")
    
    # Run 1
    meta1, nodes1, edges1, err1 = parse_python_file(target_file)
    node_ids1 = [n["node_id"] for n in nodes1]
    edge_ids1 = [e["edge_id"] for e in edges1]

    # Run 2
    meta2, nodes2, edges2, err2 = parse_python_file(target_file)
    node_ids2 = [n["node_id"] for n in nodes2]
    edge_ids2 = [e["edge_id"] for e in edges2]

    # Assertions
    assert len(nodes1) == len(nodes2), f"Node count mismatch: {len(nodes1)} vs {len(nodes2)}"
    assert len(edges1) == len(edges2), f"Edge count mismatch: {len(edges1)} vs {len(edges2)}"
    assert node_ids1 == node_ids2, "Node IDs are not deterministic!"
    assert edge_ids1 == edge_ids2, "Edge IDs are not deterministic!"
    assert meta1["file_hash"] == meta2["file_hash"], "File hash mismatch!"

    print("✅ [SUCCESS] Idempotency Verification Passed!")
    print(f"   - Total Nodes: {len(nodes1)} (Node IDs 100% matched across runs)")
    print(f"   - Total Edges: {len(edges1)} (Edge IDs 100% matched across runs)")
    print(f"   - File Hash MD5: {meta1['file_hash']}")
    print(f"   - Sample Node ID: {node_ids1[0]}")
    print(f"   - Sample Edge ID: {edge_ids1[0]}")


if __name__ == "__main__":
    test_idempotency()
