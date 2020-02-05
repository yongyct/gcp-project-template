import setuptools

setuptools.setup(
    name='gcp_project_template',
    version='0.0.1',
    author='yongyct',
    author_email='yongyct@gmail.com',
    url='https://github.com/yongyct/gcp-project-template',
    install_requires=['google-cloud-storage==1.25.0'],
    packages=setuptools.find_packages()
)
