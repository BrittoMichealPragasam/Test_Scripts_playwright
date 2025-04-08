from setuptools import setup, find_packages

setup(
    name="Test_Scripts_playwright",  # Replace with your project name
    version="0.1.0",
    description="A Playwright test automation project",
    author="Britto",  # Replace with your name
    author_email="bpragasam@tcs.woolworths.com.au",  # Replace with your email
    packages=find_packages(),
    install_requires=[
        "playwright","GCPLibrary","GCP_BigQuery_Query_Library"  # Add other dependencies here
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)