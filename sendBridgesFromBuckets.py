#!/usr/bin/python
#
# Mail out bridges from BridgeDB bucket files
#
# Copyright (c) 2011, Christian Frome <kaner@strace.org>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the <organization> nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# 

import re
import os
import sys
import smtplib
from email.mime.text import MIMEText

# Map a .brdgs file to a person's email address: Who gets what
EMAIL_MAPPING = { "PersonA.brdgs": ["foo@bar.org"],
                  "PersonB.brdgs": ["baz@baz.com"] }

# Who's on the default Cc: list?
DEFAULT_CC = ["kaner@strace.org", "arma@freehaven.net"]

# Where are the .brdgs files to be found in?
BRIDGEDB_RUN_DIR = "/home/bridges/run"

# The from field in email we send
MAIL_FROM = "tor-internal@torproject.org"

# Email text template
MAIL_TEXT = """
Hello,

Here is today's bulk of unallocated Tor Bridges.

NEW Bridges since the last email I sent you:

%s
Bridges still RUNNING since the last email I sent you:

%s
Have fun,
The Bridge Mail Bot
    """

class BridgeData:
    """Value class carrying bridge information:
       hex_key      - The unique hex key of the given bridge
       address      - Bridge IP address
       or_port      - Bridge TCP port
       distributor  - The distributor (or pseudo-distributor) through which 
                      this bridge is being announced
       first_seen   - When did we first see this bridge online?
       last_seen    - When was the last time we saw this bridge online?
       status       - One of NEW, RUNNING or OLD (see description below), 
                      initially is None

       XXX: This is a copy from bridge2bucket.py
    """
    def __init__(self, hex_key, address, or_port, distributor="unallocated",
                 first_seen="", last_seen="", status="OLD"):
        self.hex_key = hex_key
        self.address = address
        self.or_port = or_port
        self.distributor = distributor
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.status = status

def createMailBody(bridgeDict):
    """Create the text body for the emails. This more or less means we dump
       bridges marked as "NEW" or "RUNNING" into the text template.
    """

    new_bridges = "".join("      %s:%s\n" % (b.address, b.or_port) 
                            for b in bridgeDict["NEW"])
    running_bridges = "".join("      %s:%s\n" % (b.address, b.or_port) 
                            for b in bridgeDict["RUNNING"])

    if new_bridges == "":
        new_bridges = "      None\n"
    if running_bridges == "":
        running_bridges = "      None\n"

    return MAIL_TEXT % (new_bridges, running_bridges)

def readBridgesFromFile(fileName):
    """Read bridges into NEW/RUNNING/OLD dict from file.
    """
    bridgeDict = { "NEW": [], "RUNNING": [], "OLD": [] }    
    
    if not  os.path.isfile(fileName):
        print >>sys.stderr, "Not a file: %s" % fileName
        return bridgeDict

    r = re.compile('[ \t\n]+')
    try:
        f = open(fileName, 'r')
        for line in f:
            s = r.split(line)
            status = s[3]
            bridge = BridgeData(s[0], s[1], s[2], status = status)
            bridgeDict[status].append(bridge)
        f.close()
    except IOError:
        print >>sys.stderr, "IOError while reading %s" % fileName

    return bridgeDict

def sendMail(mailTo, mailBody):
    """Send a text to an address
    """

    message = MIMEText(mailBody)
    message['Subject'] = "Your daily Tor Bridges"
    message['From'] = MAIL_FROM
    message['To'] = ", ".join(mailTo)
    message['Cc'] = ", ".join(DEFAULT_CC)
    
    toAddrs = mailTo + DEFAULT_CC
    try:
       smtp = smtplib.SMTP("localhost:25")
       smtp.sendmail(MAIL_FROM, toAddrs, message.as_string())
       smtp.quit()
    except smtplib.SMTPException:
       print >>sys.stderr, "Error while trying to send to %s" % mailTo

def main():
    """Main routine: Look thorugh EMAIL_MAPPING dict and attempt to send off
       emails according to the configuration.
    """

    for k, v in EMAIL_MAPPING.items():
        bridgeDict = readBridgesFromFile(BRIDGEDB_RUN_DIR + "/" + k)
        if len(bridgeDict["RUNNING"]) > 0 or len(bridgeDict["NEW"]) > 0:
            sendMail(v, createMailBody(bridgeDict))
        else:
            print "RUNNING and NEW lists are empty. Not sending anything out."

if __name__ == "__main__":
    main()
