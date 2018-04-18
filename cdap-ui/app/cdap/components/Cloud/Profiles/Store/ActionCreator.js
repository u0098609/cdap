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

import ProfilesStore, {PROFILES_ACTIONS} from 'components/Cloud/Profiles/Store';
import {MyCloudApi} from 'api/cloud';
import fileDownload from 'js-file-download';

export const getProfiles = (namespace) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_LOADING,
    payload: {
      loading: true
    }
  });

  MyCloudApi
    .list({ namespace })
    .subscribe(
      (profiles) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_PROFILES,
          payload: { profiles }
        });
      },
      (error) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_ERROR,
          payload: { error }
        });
      }
    );
};

export const exportProfile = (namespace, profile) => {
  MyCloudApi
    .get({
      namespace,
      profile
    })
    .subscribe(
      (res) => {
        let json = JSON.stringify(res, null, 2);
        let fileName = `${profile}-profile.json`;
        fileDownload(json, fileName);
      },
      (error) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_ERROR,
          payload: { error }
        });
      }
    );
};

export const deleteProfile = (namespace, profile) => {
  let deleteObservable = MyCloudApi.delete({
    namespace,
    profile
  });
  deleteObservable.subscribe(() => {
    getProfiles(namespace);
  });
  return deleteObservable;
};
