# Garantit que "scripts" et "dags" sont importables comme des packages
# lorsque pytest est lance depuis la racine du projet (local ou Jenkins).
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))