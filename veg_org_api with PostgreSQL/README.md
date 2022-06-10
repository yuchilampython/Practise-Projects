## Building an api with python flask to create or update postgresql database for the data scrapped from the Vegetable Marketing Organization (VMO) Website


### What have I done:
![veg_org_api_diagram](https://raw.githubusercontent.com/yuchilampython/Backend-Side-Projects/main/veg_org_api/veg_org_api_diagram.PNG)
- Design the database (veg_org_api_diagram.png)
- Assign an ID number to all kinds of vegetable (all_veg_type.xlsx)
- Import all_veg_type.xlsx into postgresql database
- Download all the past data from the VMO website and import it into postgresql database (combined2012010120220429.xlsx)
- Build API for updating vegetable prices into postgresql database daily (main.py)
- Record the error in both the error log (error_log.txt) and postgresql database


### How it works?
- To use the API, user should POST json with specific format (json_post_format.txt)
- If the price of the day of the vegetable is create, then the program will automatically update the data
- Postgresql commit every row input, the program will keep running until all data are create or update or error occured in a row
- Error will be logged in the error_log.txt and postgresql database


### What to improve?
- The python code should keep running until all data have tried to POST once, and log the error into the error log, so user need not to constantly keep track of the progress
