#!/bin/zsh
# nl.aerys.heatr.daily-sweep — dagelijkse gedoseerde discovery-golf (uitbouw-modus
# Sami 2026-08-28). Enrichment-kostencap doseert de verwerking vanzelf.
cd /Users/nemesis/Heatr
/usr/bin/python3 scripts/sweep_generator.py --sector cosmetische_behandelaars --spread --max-jobs 6 --apply
/usr/bin/python3 scripts/sweep_generator.py --sector alternatieve_geneeskunde --spread --max-jobs 6 --apply
/usr/bin/python3 scripts/sweep_generator.py --sector chiropractoren --spread --max-jobs 3 --apply
