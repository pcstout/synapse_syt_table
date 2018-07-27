#!/usr/bin/env python

import sys
import os
import argparse
import getpass
import calendar
import datetime
import numpy
import synapseclient
from synapseclient import Project, Folder, File, Team, TeamMember
from synapseclient import Team, TeamMember
from synapseclient import Schema, Column, Table, Row, RowSet


class Syt:

    SYT_TABLE_NAME = 'syt_log'
    SYT_COL_USER = 'user'
    SYT_COL_ENTITY = 'entity'
    SYT_COL_CHECKED_OUT = 'checked_out'
    SYT_COL_CHECKED_IN = 'checked_in'
    SYT_COL_MESSAGE = 'message'

    def __init__(self, username=None, password=None):
        self._synapse_client = None
        self._username = username
        self._password = password

        self.synapse_login()
        self._user = self._synapse_client.getUserProfile()
        self._project = None
        self._entity = None
        self._table = None

    def checkout(self, entityId):
        """
        Checks out an Entity.
        """
        if (not self._load_entity(entityId, 'Checkout out')):
            return

        row, _, _ = self._find_checked_out_row(by_user=False)

        if (row == None):
            new_row = [[self._user.ownerId, self._entity.id,
                        self._get_synapse_timestamp(), None, None]]
            self._table = self._synapse_client.store(
                Table(self._table.id, new_row))
            print('Entity successfully checked out.')
        else:
            print('Entity is already checked out.')

    def checkin(self, entityId, message=None, force=False):
        """
        Checks in an Entity.
        """
        if (not self._load_entity(entityId, 'Checking in')):
            return

        row, etag, headers = self._find_checked_out_row(by_user=True)

        if row == None:
            print('Entity is not currently checked out.')
        else:
            print('Updating table...')
            checked_in_col = self._get_table_column_index(
                headers, self.SYT_COL_CHECKED_IN)
            message_col = self._get_table_column_index(
                headers, self.SYT_COL_MESSAGE)

            row[checked_in_col] = self._get_synapse_timestamp()
            row[message_col] = message
            self._table = self._synapse_client.store(
                Table(self._table.id, [row], etag=etag, headers=headers))
            print('Entity successfully checked in.')

    def log(self, entityId, show_all=False):
        """
        Show the check in/out log for an entity.
        """
        if (not self._load_entity(entityId, 'Show Log')):
            return
        log = self._load_syt_log(order='desc')

        user_col = self._get_table_column_index(log.headers, self.SYT_COL_USER)
        entity_col = self._get_table_column_index(
            log.headers, self.SYT_COL_ENTITY)
        checked_in_col = self._get_table_column_index(
            log.headers, self.SYT_COL_CHECKED_IN)
        checked_out_col = self._get_table_column_index(
            log.headers, self.SYT_COL_CHECKED_OUT)
        message_col = self._get_table_column_index(
            log.headers, self.SYT_COL_MESSAGE)

        for row in log:
            if not show_all and row[entity_col] != self._entity.id:
                continue
            print('-' * 80)
            print("User: {0}".format(row[user_col]))
            print("Entity Out: {0}".format(row[entity_col]))
            print("Checked Out: {0}".format(row[checked_out_col]))
            print("Checked In: {0}".format(row[checked_in_col]))
            print("Message: {0}".format(row[message_col]))

    def _find_checked_out_row(self, by_user=False):
        """
        Finds the checked out row for a user/entity where not checked in.
        """
        log = self._load_syt_log()

        user_col = self._get_table_column_index(log.headers, self.SYT_COL_USER)
        entity_col = self._get_table_column_index(
            log.headers, self.SYT_COL_ENTITY)
        checked_in_col = self._get_table_column_index(
            log.headers, self.SYT_COL_CHECKED_IN)

        checked_out_row = None

        for row in log:
            user_id = row[user_col]
            entity_id = row[entity_col]
            checked_in = row[checked_in_col]

            if ((not by_user or by_user and user_id == self._user.ownerId) and entity_id == self._entity.id and checked_in == None):
                checked_out_row = row
                break

        return [checked_out_row, log.etag, log.headers]

    def _load_syt_log(self, order='asc'):
        """
        Loads the Syt log from Synapse.
        """
        return self._synapse_client.tableQuery(
            "select * from {0} ORDER BY {1} {2}".format(
                self._table.id, self.SYT_COL_CHECKED_OUT, order)
        )

    def _load_entity(self, entityId, operationMsg):
        """
        Loads the Entity and its Project.
        """
        print('Loading Entity...')
        self._entity = self._synapse_client.get(entityId, downloadFile=False)

        type = self._entity.entityType.split('.')[-1].replace('Entity', '')

        if type in ['Folder', 'File']:
            print('Loading Project...')
            self._project = self._load_project_for(self._entity)
            print('{0} {1} {2} ({3}) from Project {4} ({5})'.format(operationMsg, type,
                                                                    self._entity.name, self._entity.id, self._project.name, self._project.id))
            self._ensure_access_log(self._project)
        else:
            print('Found {0} {1} ({2})'.format(
                type, self._entity.name, self._entity.id))
            print('Only Folders or Files can be checked in/out. Aborting.')
            self._entity = None
            self._project = None

        return self._project != None

    def _load_project_for(self, child):
        """
        Finds the Project for a child Entity.
        """
        parent = self._synapse_client.get(child.parentId)

        if (not isinstance(parent, Project)):
            parent = self._load_project_for(parent)

        return parent

    def _ensure_access_log(self, project):
        """
        Ensure that the access_log table exists in the Project.
        """
        print('Loading tables...')
        tables = self._synapse_client.getChildren(project, ["table"])

        for t in tables:
            if t['name'] == self.SYT_TABLE_NAME:
                print('Loading table...')
                self._table = self._synapse_client.get(t['id'])
                break

        if self._table == None:
            print('Creating table...')
            cols = [
                Column(name=self.SYT_COL_USER, columnType='USERID'), Column(name=self.SYT_COL_ENTITY, columnType='ENTITYID'), Column(name=self.SYT_COL_CHECKED_OUT,
                                                                                                                                     columnType='DATE'), Column(name=self.SYT_COL_CHECKED_IN, columnType='DATE'), Column(name=self.SYT_COL_MESSAGE, columnType='STRING', maximumSize=1000)
            ]
            schema = Schema(name=self.SYT_TABLE_NAME,
                            columns=cols, parent=project)
            self._table = self._synapse_client.store(schema)

    def _get_table_column_index(self, headers, column_name):
        """
        Gets the column index for a Synapse Table Column.
        """
        for index, item in enumerate(headers):
            if item.name == column_name:
                return index

    def _get_synapse_timestamp(self, date_time=datetime.datetime.utcnow()):
        """
        Gets the number of milliseconds since the epoch.
        """
        return calendar.timegm(date_time.timetuple()) * 1000

    def synapse_login(self):
        """
        Logs into Synapse.
        """
        print('Logging into Synapse...')
        syn_user = os.getenv('SYNAPSE_USER') or self._username
        syn_pass = os.getenv('SYNAPSE_PASSWORD') or self._password

        if syn_user == None:
            syn_user = input('Synapse username: ')

        if syn_pass == None:
            syn_pass = getpass.getpass(prompt='Synapse password: ')

        self._synapse_client = synapseclient.Synapse()
        self._synapse_client.login(syn_user, syn_pass, silent=True)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=[
                        'checkout', 'checkin', 'log'], help='The command to execute: checkout, checkin, or log')
    parser.add_argument('entity_id', metavar='entity-id',
                        help='The ID of the Synapse Folder or File to execute the command on.')
    parser.add_argument('-m', '--message',
                        help='A checkin message.', default=None)
    parser.add_argument('-a', '--all', help='Show all log entries for a project.',
                        default=False, action='store_true')
    parser.add_argument(
        '-f', '--force', help='Force a checkin. Can only be performed by administrators on the Synapse Project.', default=False)
    parser.add_argument('-u', '--username',
                        help='Synapse username.', default=None)
    parser.add_argument('-p', '--password',
                        help='Synapse password.', default=None)
    args = parser.parse_args()

    syt = Syt(username=args.username, password=args.password)

    if args.command == 'checkin':
        syt.checkin(args.entity_id, message=args.message, force=args.force)
    elif args.command == 'checkout':
        syt.checkout(args.entity_id)
    else:
        syt.log(args.entity_id, args.all)


if __name__ == "__main__":
    main(sys.argv[1:])
