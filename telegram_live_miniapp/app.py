#!/usr/bin/env python3
"""Compatibility entrypoint for Railway.

The unified Live ZOT application lives in combined_app.py.  Keeping this tiny
entrypoint makes both `python app.py` and `python combined_app.py` start the
same service.
"""
from combined_app import main

if __name__ == "__main__":
    main()
