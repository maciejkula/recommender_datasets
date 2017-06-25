from setuptools import setup


setup(
    name='recommender_datasets',
    version='0.1.0',
    requirements=['click', 'numpy', 'scipy', 'requests', 'h5py'],
    packages=['recommender_datasets'],
    license='MIT',
    classifiers=['Development Status :: 3 - Alpha',
                 'License :: OSI Approved :: MIT License',
                 'Topic :: Scientific/Engineering :: Artificial Intelligence'],
)
