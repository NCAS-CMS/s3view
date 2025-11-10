# Normally we build the docs by running sphhinx-build . doc from the cmamnd
# line,but this script can be used to build the docs from within Python if
# needed (e.getattr so we can use a debugger on the sphinx build process
# itself).

import sphinx.application

src_dir = "doc"       # source folder containing conf.py
build_dir = "doc/html"
doctreedir = "doc/html/.doctrees"
builder = "html"

app = sphinx.application.Sphinx(
    srcdir=src_dir,
    confdir=src_dir,
    outdir=build_dir,
    doctreedir=doctreedir,
    buildername=builder
)
app.build(force_all=True)