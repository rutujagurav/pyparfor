# Embarassingly Parallel For Loops in Python with Joblib: Convenience Warapper

I am sick of wrting the joblib boiler-plate code so I just packaged it into a general purpose function that takes as inputs - 1. a function that does the work of the body of a for loop and 2. an iterable over which the loop runs. See `demo.ipynb`.