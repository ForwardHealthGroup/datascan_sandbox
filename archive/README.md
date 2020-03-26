# automation-scripts

GitHub: https://github.com/ForwardHealthGroup/automation

For information on how to run the data scan, see `quick_guide.txt`.

### What's in this repository

* `datascan.py` is the main script which runs the data scan for a client.
* `listener.py` is the script that watches each client for new files. This
should be set up to run automatically every 30 minutes (see section below).
* `CLIENT_CONFIG.json` contains the information needed to run the data scan
automatically for each client. It contains the names and directories for each
client as well as a list of email recipients for the data scan report.
* `quick_guide.txt` gives a quick rundown on how to run the data scan manually
from the command line.

In addition, the `setup/` folder contains the files required for the database
features of the data scan:

* `setup/make_tables.sql` contains all of the SQL statements that create the
necessary tables in a client's database. If you want to add a client to the data
scan, you need to run these statements in their database.
* `dim_issue.csv` is the default list of issues for the data scan. This file
should be inserted into the `dim_issue` database table when it is first created.
* `dim_file.csv` and `dim_file_filename.csv` are used by the data scan in case
it is unable to connect to the database. They can be ignored, but should not be
deleted.

### How to set up the data scan

This repository should be cloned at `/home/development/automation/datascan/`. In
addition, there are several branches which correspond to "custom" data scans for
different clients. These branches can be cloned anywhere in the client's
respective directory (such as in their `custom_scripts` folder).

### How to set up the listener

To set `listener.py` as an automatic process that runs every 15 minutes, follow
these steps:

1. SSH into the server.
2. Run the following command (fill in the `...`s with the username and password
for the FHG service email):

```
echo -e "export SERVICE_EMAIL_USER='...';\nexport SERVICE_EMAIL_PASS='...';" >> ~/.bash_profile
```

3. Open up your crontab scheduler by typing `crontab -e`.
4. Enter the following line into the file:

```
0,30 * * * * . $HOME/.bash_profile; python /home/development/automation/listener.py
```

5. Save/close the file

This will result in `listener.py` being run on the server every 30 minutes.

### How to set up a custom data scan for a client

Suppose you want to change the way the data scan works for client X. You can
clone the data scan in client X's `custom_scripts` folder, make a new git branch
for the client, and then make the changes. The "master" data scan that runs
every 30 minutes will automatically detect this custom data scan and run it for
client X instead.

Here's what that looks like from the command line:
```
cd /home/customers/X/custom_scripts/
git clone https://github.com/ForwardHealthGroup/datascan.git
cd datascan
git checkout -b X
```
At this point, client X has a custom data scan that will run instead of the
"master" data scan. You can change whatever you like. To back up the changes you
make to GitHub, run:
```
git commit -a -m "<description of changes>"
git push origin X
```
This will commit the changes you made and then push them to X's branch of the
data scan on GitHub.

If you ever make changes to the "master" branch of the data scan, you can update
a custom branch with those changes by doing:
```
git pull origin master
```
Never merge a custom branch with the master data scan! Only merge in the other
direction. You don't want to push the custom rules for a specific client to the
main data scan.

### The files aren't being matched correctly. What can I do?

This is certainly the least effective aspect of the data scan. There are a few
ways to improve the accuracy of the file matching, but nothing is failproof:

##### Method 1

Each client should have a `dim_file` and `dim_file_filename` table in their
"test" database. (Make sure that this "test" database is listed in the
`CLIENT_CONFIG.json` file.) These tables help the data scan match files more 
accurately.

In `dim_file`, you should define the types of files you expect to receive from
the client each month. For example, suppose we expect to receive a
"demographics" file from them each month; we would add a "demographics" file to
`dim_file` and it will get assigned a unique `id`. Suppose it gets `id=3`.

Then in `dim_file_filename`, the actual filenames we receive from the client are
linked to a file type by this ID. Suppose we get a file called
"demographics_201807.csv" from the client. This filename should be entered into
`dim_file_filename` and assigned `file_id=3`.

This sounds time-consuming, but if you define the file types in `dim_file` once,
the data scan will do everything else automatically on a month-to-month basis.
However, it will get a file wrong every once in a while. If you go into
`dim_file_filename` and fix its mistake for that file, it will get it right the
next time it runs.

##### Method 2

If you want a quick-and-easy solution, just put all the new files you want to
compare in their own folder. Then, put all the old files you want to compare in
another folder. Then you can run the data scan to manually compare those two
folders:
```
scan <client_name> -o /path/to/oldfiles -n /path/to/newfiles
```
See `quick_guide.txt` for more info on how to do this.

##### Method 3

If you don't manually fix the file matching in the database from time to time,
it may throw off the file-matching. To run the data scan without it looking in
the database, run something like:
```
scan <client_name> -db none
```
You can replace "none" with anything you like. The data scan will try to find a
database called "none", fail to find it, and then run without connecting to the
database. This may improve your results.
