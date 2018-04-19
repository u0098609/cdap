/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';
import T from 'i18n-react';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import LoadingSVG from 'components/LoadingSVG';
import orderBy from 'lodash/orderBy';
import ViewAllLabel from 'components/ViewAllLabel';
import Popover from 'components/Popover';
import ConfirmationModal from 'components/ConfirmationModal';
import ProfilesStore from 'components/Cloud/Profiles/Store';
import {getProfiles, exportProfile, deleteProfile} from 'components/Cloud/Profiles/Store/ActionCreator';
import {connect, Provider} from 'react-redux';

require('./ListView.scss');

const PREFIX = 'features.Cloud.Profiles.ListView';

const PROFILES_TABLE_HEADERS = [
  {
    label: ''
  },
  {
    property: 'name',
    label: T.translate(`${PREFIX}.profileName`)
  },
  {
    property: (profile) => (profile.provisioner.name),
    label: T.translate(`${PREFIX}.provisioner`)
  },
  {
    property: 'scope',
    label: T.translate('commons.scope')
  },
  {
    property: 'pipelines',
    label: T.translate('commons.pipelines')
  },
  {
    property: 'last24HrRuns',
    label: T.translate(`${PREFIX}.last24HrRuns`)
  },
  {
    property: 'last24HrNodeHr',
    label: T.translate(`${PREFIX}.last24HrNodeHr`)
  },
  {
    property: 'totalNodeHr',
    label: T.translate(`${PREFIX}.totalNodeHr`)
  },
  {
    property: 'schedules',
    label: T.translate(`${PREFIX}.schedules`)
  },
  {
    property: 'triggers',
    label: T.translate(`${PREFIX}.triggers`)
  },
  {
    label: ''
  },
  {
    label: ''
  }
];

const SORT_METHODS = {
  asc: 'asc',
  desc: 'desc'
};

const NUM_PROFILES_TO_SHOW = 5;

class ProfilesListView extends Component {
  state = {
    profiles: this.props.profiles,
    viewAll: false,
    sortMethod: SORT_METHODS.asc,
    sortColumn: PROFILES_TABLE_HEADERS[1].property,
    profileToDelete: null,
    deleteErrMsg: '',
    extendedDeleteErrMsg: ''
  };

  static propTypes = {
    namespace: PropTypes.string.isRequired,
    profiles: PropTypes.array,
    error: PropTypes.any,
    loading: PropTypes.bool
  };

  componentDidMount() {
    getProfiles(this.props.namespace);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.profiles.length !== this.state.profiles.length) {
      this.setState({
        profiles: orderBy(nextProps.profiles, this.state.sortColumn, this.state.sortMethod)
      });
    }
  }

  toggleViewAll = () => {
    this.setState({
      viewAll: !this.state.viewAll
    });
  }

  handleProfilesSort = (field) => {
    let newSortColumn, newSortMethod;
    if (this.state.sortColumn === field) {
      newSortColumn = this.state.sortColumn;
      newSortMethod = this.state.sortMethod === SORT_METHODS.asc ? SORT_METHODS.desc : SORT_METHODS.asc;
    } else {
      newSortColumn = field;
      newSortMethod = SORT_METHODS.asc;
    }

    this.setState({
      sortColumn: newSortColumn,
      sortMethod: newSortMethod,
      profiles: orderBy(this.state.profiles, [newSortColumn], [newSortMethod])
    });
  };

  deleteProfile = (profile) => {
    deleteProfile(this.props.namespace, profile)
      .subscribe(() => {
        this.setState({
          profileToDelete: null,
          deleteErrMsg: '',
          extendedDeleteErrMsg: ''
        });
      }, (err) => {
        this.setState({
          deleteErrMsg: T.translate(`${PREFIX}.deleteError`),
          extendedDeleteErrMsg: err
        });
      });
  };

  toggleDeleteConfirmationModal = (profileToDelete = null) => {
    this.setState({
      profileToDelete,
      deleteErrMsg: '',
      extendedDeleteErrMsg: ''
    });
  }

  renderProfilesTable() {
    if (!this.state.profiles.length) {
      return (
        <div className="text-xs-center">
          {
            this.props.namespace === 'system' ?
              (
                <span>
                  {T.translate(`${PREFIX}.noProfilesSystem`)}
                  <Link to='/create-profile'>
                    {T.translate(`${PREFIX}.createOne`)}
                  </Link>
                </span>
              )
            :
              (
                <span>
                  {T.translate(`${PREFIX}.noProfiles`)}
                  <Link to={`/ns/${getCurrentNamespace()}/create-profile`}>
                    {T.translate(`${PREFIX}.createOne`)}
                  </Link>
                </span>
              )
          }

        </div>
      );
    }

    return (
      <div className="grid-wrapper">
        <div className="grid grid-container">
          {this.renderProfilesTableHeader()}
          {this.renderProfilesTableBody()}
        </div>
      </div>
    );
  }

  renderSortIcon(field) {
    if (field !== this.state.sortColumn) {
      return null;
    }

    return (
      this.state.sortMethod === SORT_METHODS.asc ?
        <IconSVG name="icon-caret-down" />
      :
        <IconSVG name="icon-caret-up" />
    );
  }

  renderProfilesTableHeader() {
    return (
      <div className="grid-header">
        <div className="grid-row sub-header">
          <div />
          <div />
          <div />
          <div />
          <div />
          <div />
          <div className="sub-title">Pipeline Usage</div>
          <div/>
          <div className="sub-title">Associations</div>
          <div/>
          <div/>
          <div/>
        </div>
        <div className="grid-row">
          {
            PROFILES_TABLE_HEADERS.map((header, i) => {
              if (header.property) {
                return (
                  <strong
                    className={classnames("sortable-header", {"active": this.state.sortColumn === header.property})}
                    key={i}
                    onClick={this.handleProfilesSort.bind(this, header.property)}
                  >
                    <span>{header.label}</span>
                    {this.renderSortIcon(header.property)}
                  </strong>
                );
              }
              return (
                <strong key={i}>
                  {header.label}
                </strong>
              );
            })
          }
        </div>
      </div>
    );
  }

  renderProfilesTableBody() {
    let profiles = [...this.state.profiles];

    if (!this.state.viewAll && profiles.length > NUM_PROFILES_TO_SHOW) {
      profiles = profiles.slice(0, NUM_PROFILES_TO_SHOW);
    }

    return (
      <div className="grid-body">
        {
          profiles.map((profile, i) => {
            return (
              <div
                className="grid-row grid-link"
                key={i}
              >
                <div></div>
                <div title={profile.name}>
                  {profile.name}
                </div>
                <div>{profile.provisioner.name}</div>
                <div>{profile.scope}</div>
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div>
                  <Popover
                    target={() => <IconSVG name="icon-cog-empty" />}
                    className="profile-actions-popover"
                    placement="bottom"
                    bubbleEvent={false}
                    enableInteractionInPopover={true}
                  >
                    <ul>
                      <li onClick={exportProfile.bind(this, this.props.namespace, profile.name)}>
                        {T.translate(`${PREFIX}.export`)}
                      </li>
                      <hr />
                      <li
                        className="delete-action"
                        onClick={this.toggleDeleteConfirmationModal.bind(this, profile.name)}
                      >
                        {T.translate('commons.delete')}
                      </li>
                    </ul>
                  </Popover>
                </div>
              </div>
            );
          })
        }
      </div>
    );
  }

  renderDeleteConfirmationModal() {
    if (!this.state.profileToDelete) {
      return null;
    }

    let confirmationElem = () => <div>{T.translate(`${PREFIX}.deleteConfirmation`, {profile: this.state.profileToDelete})}</div>;

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.deleteTitle`)}
        toggleModal={this.toggleDeleteConfirmationModal.bind(this, null)}
        confirmationElem={confirmationElem()}
        confirmButtonText={T.translate('commons.delete')}
        confirmFn={this.deleteProfile.bind(this, this.state.profileToDelete)}
        cancelFn={this.toggleDeleteConfirmationModal.bind(this, null)}
        isOpen={this.state.profileToDelete !== null}
        errorMessage={this.state.deleteErrMsg}
        extendedMessage={this.state.extendedDeleteErrMsg}
      />
    );
  }

  render() {
    if (this.props.loading) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }
    return (
      <div className="profiles-list-view">
        {
          this.props.error ?
            (
              <div className="text-danger">
                {JSON.stringify(this.props.error, null, 2)}
              </div>
            )
          :
            null
        }
        <ViewAllLabel
          arrayToLimit={this.state.profiles}
          limit={NUM_PROFILES_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderProfilesTable()}
        <ViewAllLabel
          arrayToLimit={this.state.profiles}
          limit={NUM_PROFILES_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderDeleteConfirmationModal()}
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    profiles: state.profiles,
    loading: state.loading,
    error: state.error
  };
};

const ConnectedProfilesListView = connect(mapStateToProps)(ProfilesListView);

export default function ProfilesListViewFn({...props}) {
  return (
    <Provider store={ProfilesStore}>
      <ConnectedProfilesListView {...props} />
    </Provider>
  );
}
