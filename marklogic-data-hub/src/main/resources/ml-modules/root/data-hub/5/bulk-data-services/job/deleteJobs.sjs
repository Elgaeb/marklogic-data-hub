/**
 Copyright (c) 2021 MarkLogic Corporation

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
'use strict';

xdmp.securityAssert("http://marklogic.com/data-hub/privileges/delete-jobs", "execute");

const jobs = require("/data-hub/5/impl/jobs.sjs");
const httpUtils = require("/data-hub/5/impl/http-utils.sjs");

const DEFAULT_BATCH_SIZE = 250;

var endpointState;
var endpointConstants;

const constants = fn.head(xdmp.fromJSON(endpointConstants));
const retainDuration = constants.retainDuration;

if(retainDuration == null) {
  httpUtils.throwBadRequest("retainDuration must be provided");
}

const initialState = endpointState.toObject() || {
  deleted: 0,
  retainStart: fn.currentDateTime().subtract(retainDuration),
  remaining: true,
};

const batchSize = (constants.batchSize == null || constants.batchSize == 0)
  ? DEFAULT_BATCH_SIZE
  : constants.batchSize;

const { deleted, remaining } = jobs.deleteJobDocs(initialState.retainStart, batchSize);
let res = remaining
  ? { deleted: deleted + initialState.deleted, remaining, retainStart: initialState.retainStart }
  : null;

res;
