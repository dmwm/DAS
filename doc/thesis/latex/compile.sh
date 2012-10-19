#!/bin/bash

# http://amath.colorado.edu/documentation/LaTeX/reference/faq/bibstyles.html
rm ./report_sept.pdf
pdflatex report_sept
biblex report_sept
pdflatex report_sept
pdflatex report_sept

killall evince
evince ./report_sept.pdf &
