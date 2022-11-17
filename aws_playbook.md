# Extend Volume Size
After you modify the volume size in frontend you should 
[extending OS file system](https://medium.com/@m.yunan.helmy/increase-the-size-of-ebs-volume-in-your-ec2-instance-3859e4be6cb7)

1. Type `lsblk` ; Your increased volume will be shown just above your current volume, e.g. xvda1 is your current volume with 30GB size and xvda with 40GB size.
2. Extend the partition by typing `sudo growpart /dev/xvda 1`; Note that dev/xvda is the partition name and 1 is the partition number.
3. Extend the volume by typing `sudo resize2fs /dev/xvda1`.
4. Type `df -h` to check volume size; It will show 40GB of volume size.

# Setup crontab
1. `sudo apt install cron`
2. `sudo systemctl enable cron`
3. `crontab -e` 
Remeber to copy and paste `export PATH="your current $PATH"` to the first line of your script to avoid command not found error.
4. `crontab -l` for listing all current cron jobs.

# Set up Auto Delete logs older than 30d
Add these to crontab
`find /home/ubuntu/houspiders/house_list_spider/log/*  -mtime +30 -delete`
`find /home/ubuntu/houspiders/house_list_spider/output/*  -mtime +30 -delete`
