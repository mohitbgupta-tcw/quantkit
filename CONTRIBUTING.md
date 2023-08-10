# Contributing to quantkit
All types of contributions are encouraged and valued. Please make sure to read the relevant section before making your contribution. It will make it a lot easier for the maintainers and smooth out the experience for all involved.

## GitLab
---
We use [gitlab](https://gitlab.com/tcw-group/quant-research/quantkit) to host code, to track issues and feature requests, as well as accept pull requests.

## Reporting Bugs
---
We use gitlab issues to track public bugs. Report a bug by [opening a new issue](https://gitlab.com/tcw-group/quant-research/quantkit/-/issues). A good bug report shouldn’t leave others needing to chase you up for more information. Therefore, we ask you to investigate carefully, collect information and describe the issue in detail in your report. Please complete the following steps in advance to help us fix any potential bug as fast as possible.
- Make sure that you are using the latest version
- Give a quick summary and/or background
- Give steps to reproduce the bug
- Give sample code if you can
- Explain what you expected would happen
- Explain what actually happens
- Give notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

## Suggest Code Changes
---
### Coding Standards
Please follow the [python style guide](https://ateam-bootcamp.readthedocs.io/en/latest/reference/python-style.html). This includes following [PEP 8](https://peps.python.org/pep-0008/), which outline coding standards such as:
- Modules should have short, all-lowercase names. Underscores can be used in the module name if it improves readability. Python packages should also have short, all-lowercase names, although the use of underscores is discouraged. 
- Class names should normally use the CapWords convention. 
- Function names should be lowercase, with words separated by underscores as necessary to improve readability. 
- Variable names follow the same convention as function names. 
- Make sure to add commentary to your code and explain the functionality of functions and classes.

Before submitting code, run the `black` package to reformat your code by copying the following command into your command line from the project folder.
```shell
> python -m black .
```


### Create Pull Requests
Pull requests are the best way to propose changes to the codebase. 

- Pull the repository (See Installation Steps in [README](README.md))
- Create a branch from develop
```shell
> git branch <new-branch-name> develop
```
- Make your code changes in that branch
- Run test cases. Does the code still work without throwing errors? Does it return the expected outputs?
- Update documentation if applicable. If you made fundamental changes to the code, make sure to add a note in the documentation in [README](README.md). 
- Make sure you follow the [coding standards](#coding-standards) and run `black` to format code. 
- Use `git commit` to commit your code locally. Make sure to explain your changes in a short message. 
```shell
> git commit -am "short message to explain change"
```
- Use `git push` to push your changes to the remote repository.
```shell
> git push -u origin <new-branch-name>
```
- Create a pull request in gitlab. Go to the [merge request](https://gitlab.com/tcw-group/quant-research/quantkit/-/merge_requests) site in gitlab and open a new merge request. As Source branch, choose your branch, as target branch develop. Click Compare branches and continue. Title the pull request with a short description of the changes made and the issue or bug number associated with your change. For example, you can title an issue like so "Added more functionality to resolve #12". In the description of the pull request, explain the changes that you made, any issues you think exist with the pull request you made, and any questions you have for the maintainer. As Assignee and Reviewer, choose Tim Bastian.
