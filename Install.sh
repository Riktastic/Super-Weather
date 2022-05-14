#!/bin/sh
printf "\n#############################\nInstalling the requirements for Super-Weather.\n#############################\n\n"
sudo apt update # Check if the list of mirrors is up to date.
sudo apt install python3 python3-pip rtl-433 libpq-dev # Install the required applications.
python3 -m pip install --upgrade pip # Upgrade pip
python3 pip install -r ./requirements.txt # Install the required Python packages.
printf "\n#############################\nIf no errors have been found,\nyou can now edit the configuration file\nand start collecting weatherdata.\n############################\n"
