# Introduction 
Code contains sample project structure for hosting GCP projects (python only for now)

# Getting Started
## Prerequisites
The following packages should be installed (via `pip install`):
* `google-cloud-storage`
* `apache-beam[gcp]`
Service account json should also be configured, by pointing the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of the service account json. [See here for more details](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable).

## Project Structure
Files should be organized and added based on the below structure:
* `composer` folder contains the DAGs for cloud composer
* `dataflow` folder contains the scripts for starting individual data pipelines in cloud dataflow
* `etc` folder contains any miscellaneous files (e.g. configs, static files, etc.)
	* for dataflow scripts, to create a corresponding config `.yml` file with name of `[dataflow script name]_[environment].yml`
* `tests` unit test case for scripts

## Example Usage
From the project root folder, execute `python -m dataflow.sample_dataflow --environment dev`. In this sample, files from configured paths in `etc/dataflow_dev.yml` in cloud storage will be processed, validated and deposited in an output gcs path, as configured.
Project has been structured to also work with cloud composer, simply pipe contents in this repo into the `DAGS folder - /dags` of cloud composer's managed cloud storage.

# Build and Test
TODO: Add sample unit test
