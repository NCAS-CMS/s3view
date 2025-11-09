import inspect
import inspect
import textwrap
import cmd2
from sphinx.ext.autodoc import FunctionDocumenter, ClassDocumenter, ModuleDocumenter, MethodDocumenter
from sphinx.ext.autosummary import Autosummary
from docutils import nodes
from docutils.statemachine import ViewList
from sphinx.util.nodes import nested_parse_with_titles
import sphinx.ext.autodoc.directive as autodocdir
from sphinx import addnodes
import re

# ---------------------------------------------------------------------
# Helper functions for argparse formatting
# ---------------------------------------------------------------------

def format_argparser_summary(parser):
    """Return short one-line summary of parser arguments for table display."""
    parts = []
    for a in parser._actions:
        if getattr(a, "help", None) == "==SUPPRESS==":
            continue
        if not getattr(a, "option_strings", None):
            parts.append(a.dest)
        else:
            parts.append("/".join(a.option_strings))
    return ", ".join(parts) if parts else ""


def format_argparser_rst(parser):
    """Return full RST-formatted argument list for docstrings."""
    lines = []
    positionals = [a for a in parser._actions if not a.option_strings and a.help != "==SUPPRESS=="]
    if positionals:
        lines.append("**Positional Arguments:**")
        for a in positionals:
            lines.append(f"- ``{a.dest}``: {a.help or ''}")

    optionals = [a for a in parser._actions if a.option_strings and a.help != "==SUPPRESS=="]
    if optionals:
        lines.append("")
        lines.append("**Optional Arguments:**")
        for a in optionals:
            opts = ", ".join(a.option_strings)
            lines.append(f"- ``{opts}``: {a.help or ''}")

    return [textwrap.fill(line, width=90) if not line.startswith("**") else line for line in lines]

#---------------------------------------------------------------------
# Custom Class Documenter for cmd2 applications
# ---------------------------------------------------------------------

class Cmd2ClassDocumenter(ClassDocumenter):
    """
    Documenter for cmd2.Cmd subclasses that only shows do_* commands.
    
    Usage:
        .. autocmd2:: mymodule.MyCmd2App
           :members:
    """
    objtype = "cmd2"
    directivetype = "class"
    priority = ClassDocumenter.priority + 10
    member_documenter = "cmd2method"  
    
    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        """Only document cmd2.Cmd subclasses."""
        return inspect.isclass(member) and issubclass(member, cmd2.Cmd)
    
    def filter_members(self, members, want_all):
        """Filter to only show user-defined do_* methods."""
        ret = []

        for member_obj in members:

            # member_objs are defined in 
            # https://github.com/sphinx-doc/sphinx/blob/62619bd5a2e8b203ff81b50456508bd7d2583920/sphinx/ext/autodoc/_member_finder.py#L51

            membername = member_obj.__name__

            # Skip methods inherited from base cmd2.Cmd
            if hasattr(cmd2.Cmd, membername):
                continue

            # Only keep do_* methods
            if not membername.startswith("do_"):
                continue
            
            # The boolean indicates that this is not an attribute, it's a method
            # Code from https://github.com/sphinx-doc/sphinx/blob/master/sphinx/ext/autodoc/_member_finder.py#L427
            
            ret.append( (membername, member_obj, False))
        
        return ret

    
    def add_content(self, more_content=None):
        """
        Emit the normal docstring, then a table of do_* commands.
        """
        super().add_content(more_content)

        check, all_members = self.get_object_members(want_all=True)
        members = self.filter_members(all_members,want_all=True)

        if not members:
            return

        # Build the RST table
        lines = [
            "",
            ".. list-table:: Commands",
            "   :header-rows: 1",
            "",
            "   * - Command",
            "     - Description"
        ]

        rows = []
        for name, member, _ in members:
            cmd_name = name[3:]
            func = getattr(self.object, name, None)
            if func is None:
                desc="error"
            else:
                desc = inspect.getdoc(func) or ""
                try:
                    desc = desc.split('\n')[0]
                except:
                    desc="no one line doc string summary provided"

            desc = desc.replace("\n", " ")
            lines.append(f"   * - ``{cmd_name}``")
            lines.append(f"     - {desc}")
    
        for line in lines:
            self.add_line(line,"<autocmd2>")
    



# ---------------------------------------------------------------------
# Custom Method Documenter for cmd2 commands
# ---------------------------------------------------------------------
class Cmd2MethodDocumenter(MethodDocumenter):
    """
    Documenter for do_* methods in cmd2 classes.
    Strips the 'do_' prefix and adds argparse documentation.
    """
    objtype = "cmd2method"
    directivetype = "method"
    priority = MethodDocumenter.priority + 10
    
    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        """Document do_* methods in cmd2 classes."""

        print('can_document_member:', membername)

        #if not MethodDocumenter.can_document_member(member, membername, isattr, parent):
        #    return False
        
        # Check if parent is a cmd2.Cmd subclass
        #parent_obj = getattr(parent, "object", None)
        #if not (inspect.isclass(parent_obj) and issubclass(parent_obj, cmd2.Cmd)):
        #    return False
        
        # Only document do_* methods
        if not membername.startswith("do_"):
            return False
        
        # Skip base cmd2.Cmd methods
        if hasattr(cmd2.Cmd, membername):
            return False
        
        print('will_document_member:', membername)

        return True
    
    def format_name(self):
        """Strip 'do_' prefix from command name."""
        name = getattr(self.object, "__name__", "")
        if name.startswith("do_"):
            return name[3:]
        return super().format_name()
    
    def get_signature(self, *args, **kwargs):
        """ We want a command line signature """
        parser = getattr(self.object, "argparser", None)
        if not parser:
            return " "
        # Positional arguments
        pos = [a.dest for a in parser._actions if not a.option_strings]
        # Optional flags
        opts = ["/".join(a.option_strings) for a in parser._actions if a.option_strings]
        parts = pos + opts
        print (parts)
        return f" {' '.join(parts)}"

    
    def add_content(self, more_content=None):
        """Add docstring and argparse documentation."""
        # Add the docstring
        super().add_content(more_content)
        
        # Add argparse documentation if available
        parser = getattr(self.object, "argparser", None)
        if not parser:
            return
        
        lines = ["", "**Command Arguments:**"]
        lines.extend(format_argparser_rst(parser))
        
        for line in lines:
            self.add_line(line, "<autocmd2>")

    def add_directive_header(self, sig=None):
        # Force signature computation via get_signature()
        super().add_directive_header(sig=self.get_signature())




# ---------------------------------------------------------------------
# Sphinx setup
# ---------------------------------------------------------------------
def setup(app):
    """Register the cmd2 documenters."""
    app.add_autodocumenter(Cmd2ClassDocumenter)
    app.add_autodocumenter(Cmd2MethodDocumenter)

    
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }