import ast
import hashlib
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional


def generate_node_id(file_path: str, node_type: str, lineno: int, col_offset: int, name: str = "") -> str:
    """Generate a stable, deterministic identifier for a code node."""
    raw_str = f"{file_path}:{node_type}:{lineno}:{col_offset}:{name}"
    return hashlib.sha256(raw_str.encode('utf-8')).hexdigest()[:16]


def generate_edge_id(source_node_id: str, edge_type: str, target_node_id: str) -> str:
    """Generate a stable, deterministic identifier for a graph edge."""
    raw_str = f"{source_node_id}:{edge_type}:{target_node_id}"
    return hashlib.sha256(raw_str.encode('utf-8')).hexdigest()[:16]


def get_iso_timestamp(mtime: Optional[float] = None) -> str:
    """Return ISO 8601 formatted timestamp in UTC."""
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc) if mtime else datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class CPGVisitor(ast.NodeVisitor):
    """AST Visitor to extract CPG (AST, CFG, DFG, Call edges) from a Python AST."""

    def __init__(self, file_path: str, schema_version: str = "1.0"):
        self.file_path = file_path
        self.schema_version = schema_version
        self.nodes: List[Dict[str, Any]] = []
        self.edges: List[Dict[str, Any]] = []
        self.node_id_map: Dict[ast.AST, str] = {}
        self.parent_stack: List[str] = []
        self.var_definitions: Dict[str, str] = {}  # var_name -> node_id of definition

    def _add_node(self, node: ast.AST, label: str, name: str = "") -> str:
        lineno = getattr(node, 'lineno', 0)
        col_offset = getattr(node, 'col_offset', 0)
        node_type = type(node).__name__

        node_id = generate_node_id(self.file_path, node_type, lineno, col_offset, name)
        self.node_id_map[node] = node_id

        node_event = {
            "schema_version": self.schema_version,
            "event_time": get_iso_timestamp(),
            "node_id": node_id,
            "node_label": label,
            "properties": {
                "file_path": self.file_path,
                "ast_type": node_type,
                "name": name,
                "line_number": lineno,
                "col_offset": col_offset
            }
        }
        self.nodes.append(node_event)
        return node_id

    def _add_edge(self, source_id: str, target_id: str, edge_type: str, properties: Optional[Dict[str, Any]] = None):
        edge_id = generate_edge_id(source_id, edge_type, target_id)
        edge_event = {
            "schema_version": self.schema_version,
            "event_time": get_iso_timestamp(),
            "edge_id": edge_id,
            "source_node_id": source_id,
            "target_node_id": target_id,
            "edge_type": edge_type,
            "properties": properties or {}
        }
        self.edges.append(edge_event)

    def visit(self, node: ast.AST):
        node_name = getattr(node, 'name', getattr(node, 'id', ''))
        
        # Determine label based on AST node type
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            label = "FUNCTION"
        elif isinstance(node, ast.ClassDef):
            label = "CLASS"
        elif isinstance(node, ast.Name):
            label = "VARIABLE"
        elif isinstance(node, (ast.Call, ast.Attribute)):
            label = "EXPRESSION"
        else:
            label = f"AST_{type(node).__name__.upper()}"

        current_node_id = self._add_node(node, label, str(node_name))

        # Add AST parent-child edge
        if self.parent_stack:
            parent_id = self.parent_stack[-1]
            self._add_edge(parent_id, current_node_id, "AST")

        # Track Variable definition (DFG)
        if isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Store):
                self.var_definitions[node.id] = current_node_id
            elif isinstance(node.ctx, ast.Load):
                if node.id in self.var_definitions:
                    def_node_id = self.var_definitions[node.id]
                    self._add_edge(def_node_id, current_node_id, "DFG", {"variable": node.id})

        # Track Call edges
        if isinstance(node, ast.Call):
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            if func_name and self.parent_stack:
                caller_id = self.parent_stack[-1]
                self._add_edge(caller_id, current_node_id, "CALLS", {"callee_name": func_name})

        # Push to stack and process children
        self.parent_stack.append(current_node_id)
        
        # Handle CFG extraction for body statements
        if hasattr(node, 'body') and isinstance(node.body, list):
            self._extract_cfg_for_block(node.body)
            
        super().generic_visit(node)
        self.parent_stack.pop()

    def _extract_cfg_for_block(self, statements: List[ast.AST]):
        """Extract Control Flow Graph (CFG) edges between sequential statements."""
        prev_node_id = None
        for stmt in statements:
            stmt_lineno = getattr(stmt, 'lineno', 0)
            stmt_col = getattr(stmt, 'col_offset', 0)
            stmt_name = getattr(stmt, 'name', getattr(stmt, 'id', ''))
            stmt_id = generate_node_id(self.file_path, type(stmt).__name__, stmt_lineno, stmt_col, str(stmt_name))
            
            if prev_node_id:
                self._add_edge(prev_node_id, stmt_id, "CFG")
            prev_node_id = stmt_id


def parse_python_file(file_path: str, repo_root: str = "") -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Parse a single Python file into CPG events.
    Returns: (metadata_event, node_events, edge_events, error_event)
    """
    rel_path = os.path.relpath(file_path, repo_root) if repo_root else file_path
    rel_path = rel_path.replace("\\", "/")

    try:
        stat_info = os.stat(file_path)
        last_modified = get_iso_timestamp(stat_info.st_mtime)

        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        file_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        loc = len(content.splitlines())

        tree = ast.parse(content, filename=file_path)
        visitor = CPGVisitor(file_path=rel_path)
        visitor.visit(tree)

        metadata_event = {
            "schema_version": "1.0",
            "event_time": get_iso_timestamp(),
            "file_path": rel_path,
            "file_hash": file_hash,
            "loc": loc,
            "parse_status": "SUCCESS",
            "last_modified": last_modified
        }

        return metadata_event, visitor.nodes, visitor.edges, None

    except Exception as e:
        error_event = {
            "schema_version": "1.0",
            "event_time": get_iso_timestamp(),
            "file_path": rel_path,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": getattr(e, '__traceback__', None) and str(e)
        }

        metadata_event = {
            "schema_version": "1.0",
            "event_time": get_iso_timestamp(),
            "file_path": rel_path,
            "file_hash": "",
            "loc": 0,
            "parse_status": "FAILED",
            "last_modified": get_iso_timestamp()
        }

        return metadata_event, [], [], error_event
