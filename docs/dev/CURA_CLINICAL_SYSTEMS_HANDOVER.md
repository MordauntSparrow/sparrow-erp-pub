# Cura Clinical Systems: System Summary for Cursor Backend Redesign

## Purpose

This document describes the current frontend system as it exists now, so Cursor can redesign the backend to match the live UI rather than the older backend assumptions.

This file is intended to be the single source of truth for the current system. Cursor should use this one file as the complete backend redesign brief for:

- routes
- modules
- UI workflows
- data models
- existing endpoints
- missing endpoints
- offline sync requirements
- event manager requirements
- safeguarding requirements
- minor injury requirements
- EPCR requirements

The app is now a React/Vite application with three active modules:

1. EPCR
2. Safeguarding
3. Minor Injury

The frontend is partly API-backed today and partly local-storage-backed. The backend redesign needs to unify that into a complete, production-ready API.

## Critical Non-Negotiable Requirement: No Data Loss

Because EPCR, safeguarding, and minor injury forms contain clinically sensitive and safeguarding-sensitive information, the redesigned system must be offline-first and crash-safe.

This is not optional.

Cursor should assume the following are mandatory requirements:

1. No submitted or draft form data can be lost because of poor signal, refresh, tab close, app crash, browser restart, or temporary backend outage.
2. Safeguarding and Minor Injury must have the same offline resilience expectations as EPCR.
3. Every create, update, submit, and close action must be durably queued locally until the server confirms receipt.
4. The UI must always be able to show the user whether a record is:
   - local draft only
   - pending sync
   - synced
   - sync failed
   - conflict detected
5. Backend APIs must support idempotent replay, because the client will retry aggressively until it gets confirmation.

## Required Offline-First Architecture

All three modules must support:

- durable local draft persistence
- durable pending-sync queue
- automatic retry when connection returns
- explicit sync status on each record
- idempotent server writes
- audit-safe conflict handling

Recommended local client model per record:

- `localId`
- `serverId`
- `module`
- `status`
  - `draft`
  - `pending_create`
  - `pending_update`
  - `pending_submit`
  - `pending_close`
  - `synced`
  - `sync_failed`
  - `conflict`
- `lastModifiedAt`
- `lastSyncedAt`
- `syncAttempts`
- `lastSyncError`
- `idempotencyKey`
- `deviceId`
- `createdBy`

Recommended queue model:

- append-only pending operations queue
- each queued operation stores:
  - `operationId`
  - `recordLocalId`
  - `recordServerId`
  - `operationType`
  - `payload`
  - `createdAt`
  - `attemptCount`
  - `lastAttemptAt`
  - `idempotencyKey`
  - `dependencyOperationId` if needed

Recommended backend requirements for offline replay:

- accept client-generated idempotency keys
- support upsert-safe create/update semantics
- return stable server identifiers
- reject duplicates safely without data corruption
- preserve server-side audit trail for retries and merges

## Current Technical Shape

- Frontend stack: `React 19`, `TypeScript`, `MUI`, `react-router-dom`
- Local persistence:
  - `localStorage` for user, ePCR draft state, safeguarding referrals, legacy settings
  - `localforage` for datasets and app settings
- Current backend plugins referenced by frontend:
  - `https://systems-api.platinumambulance.co.uk/plugin/medical_records_module`
  - `https://systems-api.platinumambulance.co.uk/plugin/minor_injury_module`

## Active Frontend Route Map

### Public

- `/login`
- `/settings`

### Protected

- `/modules`
- `/`
- `/epcr-module`
- `/epcr-module/airway`
- `/epcr-module/breathing`
- `/epcr-module/circulation`
- `/epcr-module/disability`
- `/epcr-module/exposure`
- `/epcr-module/observations`
- `/epcr-module/drugs`
- `/epcr-module/review`
- `/epcr-module/patient-info`
- `/epcr-module/presenting-complaint`
- `/epcr-module/pain-history`
- `/epcr-module/past-medical-history`
- `/epcr-module/drug-history`
- `/epcr-module/family-social-history`
- `/epcr-module/systems-review`
- `/epcr-module/needs`
- `/epcr-module/clinical-handover`
- `/epcr-module/catastrophic`
- `/epcr-module/falls`
- `/epcr-module/femur-fracture`
- `/epcr-module/hypoglycaemia`
- `/epcr-module/oohca`
- `/epcr-module/stroke`
- `/epcr-module/incident-log`
- `/epcr-module/billing`
- `/safeguarding`
- `/safeguarding/form/:id`
- `/minor-injury`

## Authentication State

### Current frontend behavior

- Login is currently mocked in the UI.
- Valid demo credentials are hardcoded:
  - username: `trial-user`
  - password: `demo0925`
- On success, the UI stores:
  - `user.id`
  - `user.username`
  - `user.name`

### Backend that now needs to exist

- Real auth endpoint
- Session/token refresh
- `me` endpoint
- logout/invalidate endpoint
- role support:
  - clinician
  - event lead / control
  - safeguarding reviewer
  - admin

## Core Backend Domains

The backend should be split into these domains:

1. Auth and user profile
2. EPCR case management
3. Datasets and app configuration
4. Safeguarding referrals
5. Minor injury reports
6. Event manager / event operations
7. File upload / document storage
8. Reference data and analytics

## Existing EPCR API Surface Already Used by Frontend

Base:

- `/plugin/medical_records_module/api`

### Health

- `HEAD /ping`

Used by:

- connection checks
- offline/online banner logic
- sync retry logic

### EPCR Cases

- `POST /cases`
- `GET /cases?username={username}`
- `GET /cases/{caseId}`
- `PUT /cases/{caseId}`
- `PUT /cases/{caseId}/close`

### EPCR offline sync is mandatory

EPCR already has partial offline behavior in the current frontend, but the backend redesign must formalize it properly.

Required EPCR offline behaviors:

1. Case creation must work offline.
2. Case section updates must persist locally immediately.
3. Case close must queue safely if the connection is unavailable.
4. Pending creates and pending updates must survive refresh, crash, browser close, and restart.
5. Polling must never overwrite newer unsynced local edits without conflict handling.
6. Duplicate create/update/close attempts must be idempotent.

Required EPCR sync endpoints or equivalent write semantics:

- idempotent `POST /cases`
- idempotent `PUT /cases/{caseId}`
- idempotent `PUT /cases/{caseId}/close`
- optional `POST /cases/sync`
- optional `POST /cases/{caseId}/sync-close`

Required EPCR sync metadata:

- `syncStatus`
- `pendingCreate`
- `pendingUpdate`
- `pendingClose`
- `lastLocalSaveAt`
- `lastServerAckAt`
- `syncError`
- `version`

### Patient lookup

- `GET /search?date_of_birth=YYYY-MM-DD&postcode={postcode}`

This is the VITA/urgent-care member search used by Patient Info.

### Datasets

- `GET /datasets/versions`
- `GET /datasets/{datasetName}`

Datasets currently used by UI:

- `drugs`
- `clinicalOptions`
- `ivFluids`
- `clinicalIndicators` is present in local dataset service and should also be supported server-side

## EPCR Main Data Model

### Case envelope

Each EPCR case is currently shaped like:

```ts
interface EpcrCase {
  id?: string | number;
  createdAt?: string;
  closedAt?: string;
  status?: string;
  assignedUsers?: string[];
  updatedAt?: string;
  sections: EpcrSection[];
}

interface EpcrSection {
  name: string;
  content: any;
}
```

### Required case-level fields

- `id`
- `status`
  - `In Progress`
  - `Closed`
- `createdAt`
- `updatedAt`
- `closedAt`
- `assignedUsers`

### Required section names

These exact names are used in the UI and should be preserved or mapped server-side:

- `PatientInfo`
- `Needs`
- `OBS`
- `Airway`
- `B Breathing and Chest`
- `C Circulation`
- `D Disability`
- `<C> Catastrophic Haemorrhage`
- `Presenting Complaint / History`
- `Past Medical History`
- `Drugs History and Allergies`
- `Family / Social History`
- `Systems Reviews`
- `Pain History (SOCRATES)`
- `Clinical Handover`
- `Incident Log`
- `Billing`
- `Falls In Older Adults`
- `Neck of Femur Fracture`
- `Hypoglycaemia`
- `OOHCA`
- `Stroke`

## EPCR UI Surface and Required Data

### Dashboard / case list

UI needs:

- list cases for logged-in user
- patient name derived from `PatientInfo.ptInfo.forename/surname`
- status
- created date
- open existing case
- create case
- create offline case

### Patient Info

Section name:

- `PatientInfo`

Content currently contains:

- `ptInfo`
  - `forename`
  - `surname`
  - `nhsNumber`
  - `dob`
  - `age`
  - `gender`
  - `genderOther`
  - `ethnicity`
  - `urgentCareMember`
    - `isMember`
    - `membershipNumber`
    - `primaryType`
    - `membershipLevel`
  - `homeAddress`
    - `address`
    - `postcode`
    - `telephone`
  - `gpSurgery`
    - `address`
    - `gpName`
    - `surgeryName`
    - `postcode`
  - `nextOfKinRecords[]`
    - `forename`
    - `surname`
    - `telephone`
    - `relationship`
    - `relationshipOther`
    - `lpaHealth`
- `capacityRecords[]`
  - `timestamp`
  - `understandingStatus`
  - `capacityOtherDetails`
  - `diagnosticAssessmentDecision`
  - `patientUnderstands`
  - `patientRetains`
  - `patientWeighs`
  - `patientCommunicates`
  - `patientCapacity`
- `consentRecords[]`
  - `timestamp`
  - `consentType`
  - `consentDetails`
  - `consentGiven`
- `refusalRecords[]`
  - `timestamp`
  - `refusalType`
  - `refusalReason`
  - `risksExplained`
  - `adviceGiven`
  - `patientSignature`

Also required:

- VITA search by DOB + postcode
- ability to merge returned patient data into `ptInfo`

### Needs

- `supportOptions[]`
- `supportingDocumentation`
- `uploadedDocs[]`

Current UI allows image upload, compression, preview, delete.
Backend needs file upload/storage for this instead of raw base64 in-browser.

### Case Review

The Review UI currently captures:

- `onArrival`
- `treatmentInterventions`
- `plan`
- `safetyNetting`
- `worseningCareAdvice`

This data is currently not properly persisted into a named EPCR section and should be formalized server-side, for example:

- section name `Review`

### Catastrophic Haemorrhage

- `records[]`
  - `timeApplied`
  - `dressingType`
  - `siteApplied`
  - `otherDetail`

### Airway

- `airwayRecords[]`
  - `timeOfAssessment`
  - `assessment`
  - `details`
- `treatmentRecords[]`
  - `timestamp`
  - `selected[]`
  - `other`
- `igelRecords[]`
  - `timestamp`
  - `size`
  - `success`
  - `endTidalCO2`
- `intubationRecords[]`
  - `timestamp`
  - `etTubeSize`
  - `success`
  - `initialEndTidalCO2`
  - `oesophagealIntubation`
- `needleRecords[]`
  - `timestamp`
  - `attempts`
  - `success`
- `quickTrachRecords[]`
  - `timestamp`
  - `attempts`
  - `success`
  - `initialEndTidalCO2`

### Breathing and Chest

- top-level:
  - `oxygenGiven`
- `breathingRecords[]`
  - `id`
  - `timeOfAssessment`
  - `respiratoryRate`
  - `spO2`
  - `etco2`
  - `oxygenGiven`
  - `breathingAdequately`
  - `other`
  - `chestView`
  - `anteriorMarkers[]`
  - `posteriorMarkers[]`
  - `chestNotes`

Each chest marker contains:

- `x`
- `y`
- `sign`
- `id`

### Circulation

- `circulationRecords[]`
  - `id`
  - `timeOfAssessment`
  - `rhythm`
  - `arm`
  - `mostDistalPulse`
  - `pulseRate`
  - `pulseQuality`
  - `systolic`
  - `diastolic`
  - `bpPosition`
  - `capillaryRefill`
  - `centralPeripheral`
  - `ecg[]`
  - `ecgImage`
- `skinForm`
  - `appearance`
  - `texture`
  - `temperature`
- `ivAccessRecords[]`
  - `id`
  - `insertionSite`
  - `otherInsertionSite`
  - `cannulationTime`
  - `cannulaSize`
  - `attempts`
  - `successful`
  - `aseptic`
- `ioAccessRecords[]`
  - same shape as IV access
- `generalComments`

### Disability

- `disabilityRecords[]`
  - `id`
  - `timeOfAssessment`
  - `gcs`
  - `eye`
  - `verbal`
  - `motor`
  - `bloodGlucose`
  - `leftPupilSize`
  - `rightPupilSize`
  - `leftPupilReactive`
  - `rightPupilReactive`
  - `acvpu`
  - `details`

### Exposure

- `temperatureRecords[]`
  - `timeOfAssessment`
  - `value`
- `exposureComments`
- `markers`
  - keyed by injury type:
    - `Wounds`
    - `Pain`
    - `Burns`
    - `Fracture`
    - `Dislocation`
    - `Spinal Injury`

Each exposure marker contains:

- `x`
- `y`
- `injuryType`
- `id`
- `details`

Marker `details` vary by subtype, including:

- wounds:
  - `woundType`
  - `treatmentTypes[]`
- pain:
  - `timeOfPain`
  - `painScore`
- burns:
  - `burnThickness`
  - `burnType`
  - `burnTreatments[]`
  - `bodyPercentage`
- fracture/dislocation:
  - `fractureType`
  - `fractureTreatments[]`
  - `treatments[]`
  - `distalChecks[]`
- spinal:
  - `assessment1[]`
  - `assessment2[]`
  - `extrication[]`
  - `extricationOther`

### Global Observations

Section:

- `OBS`

Contains:

- `globalObservations[]`

Each observation record contains:

- `id`
- `timeOfAssessment`
- `pulse`
- `pulseQuality`
- `respRate`
- `sbp`
- `dbp`
- `spO2`
- `spo2Scale`
- `oxygenGiven`
- `capRefill`
- `centralPeripheral`
- `rhythm`
- `ecg`
- `arm`
- `mostDistalPulse`
- `gcs`
- `gcsEye`
- `gcsVerbal`
- `gcsMotor`
- `gcsBreakdown`
- `acvpu`
- `etco2`
- `temperature`
- `bloodGlucose`
- `leftPupil`
- `rightPupil`
- `leftPupilReactive`
- `rightPupilReactive`
- `pef`
- `airway`
- `breathingAdequate`
- `comments`
- `score`
- `scoreType`

The UI calculates:

- `NEWS2` for adults
- `POPS` for paediatrics

Backend should persist calculated score and also be able to recalculate safely server-side.

### Drugs Administered

The UI currently stores only local state; backend should formalize a section, for example:

- section name `Drugs Administered`

Each record contains:

- `id`
- `drugName`
- `dosage`
- `unit`
- `batchNumber`
- `expiryDate`
- `administeredBy`
- `route`
- `timeAdministered`
- `notes`
- `costConsent`
- `cost`

The UI also calculates:

- total drugs cost
- overall total with a base callout fee

### Presenting Complaint / History

- `complaintDescription`
- `history`
  - `symptomsDescription`
  - `timingAndDuration`
  - `exacerbatingAndRelievingSymptoms`
  - `associatedSymptoms`
  - `otherQuestionsAndResponses`

### Past Medical History

- `records[]`
  - `condition`
  - `diagnosedDate`
  - `severity`
  - `unknownDiagnosedDate`
- `comments`

### Drugs History and Allergies

- `drugs[]`
  - `drugName`
  - `dosage`
  - `frequency`
  - `lastTaken`
- `allergies[]`
  - `allergen`
  - `reaction`
  - `severity`
  - `lastReacted`

### Family / Social History

- `familyRecords[]`
  - `relationship`
  - `condition`
  - `ageOrOutcome`
- `livingArrangement`
  - `livesAlone`
  - `carers`
  - `carersAttendAmount`
  - `otherArrangements`
  - `environmentalFactors[]`
  - `otherEnvironmental`
- `occupation`
  - `occupation`
  - `status`
- `adl`
  - `generalAppearance`
  - `mobility`
  - `walkingAid`
  - `wheelchair`
  - `otherMobility`
  - `languageRequirements`
  - `otherCommunication`
- `alcohol`
  - `unitsPerWeek`
  - `lastDrank`
  - `amountAndWhen`
  - `signs[]`
- `smoking`
  - `packsPerWeek`
  - `yearsSmoking`
  - `packYears`
- `lifestyle`
  - `diet`
  - `exercise`
  - `travel`
- `socialHistory`
  - `tobaccoUse`
  - `alcoholUse`
  - `drugUse`
  - `suspectedAbuse`

### Systems Reviews

- `systemsReviewData.records[]`
  - `system`
  - `reviewResult`
  - `details`
- `generalReviewData`
  - `weightLoss`
  - `appetiteChange`
  - `lumpsOrBumps`
  - `rashes`
  - `jointPain`
  - `otherReview`

### Pain History (SOCRATES)

- `records[]`
  - `site`
  - `onset`
  - `character`
  - `radiation`
  - `associations`
  - `timing`
  - `exacerbatingRelieving`
  - `severity`

### Clinical Handover / Summary

- `outcome`
- `otherOutcomeDetails`
- `nhsAmbulance`
  - `trustName`
  - `callSign`
  - `cadNumber`
- `receivingHospital`
  - `hospital`
  - `ward`
  - `otherWard`
  - `otherHospitalDetails`
- `privateAmbulance`
  - `companyName`
  - `callSign`
  - `cadNumber`
- `primaryClinician`
  - `name`
  - `signature`
- `dischargeClinician`
  - `name`
  - `role`
  - `signature`
- `others[]`
  - `name`
  - `role`
  - `signature`
- `adviceDoc`
  - `front`
  - `back`
- `summaryNotes`

This section is also the close-case workflow trigger.

### Incident Log

- `responseType`
- `responseOther`
- `callSign`
- `cadNumber`
- `caseNumber`
- `caseCreatedAt`
- `pinCode`
- `timeOfCall`
- `timeMobile`
- `timeAtScene`
- `timeLeaveScene`
- `timeAtHospital`
- `timeHandover`
- `timeLeftHospital`
- `timeAtTreatmentCentre`
- `timeOfPrealert`

### Billing

- `payeeName`
- `payeeEmail`
- `payeeAddress`
- `notes`

### Falls In Older Adults

- `riskFactorCategory`
- `longLie`
- `estimatedLongLieTime`
- `newOnsetConfusion`
- `posturalHypotension`
- `bpLyingDownSystolic`
- `bpLyingDownDiastolic`
- `bpStanding1MinSystolic`
- `bpStanding1MinDiastolic`
- `bpStanding3MinSystolic`
- `bpStanding3MinDiastolic`
- `mobilityAssessment`
- `fallReferralMade`
- `fallReferralTo`
- `fallReferralOther`
- `clinicalFrailty`

### Neck of Femur Fracture

- `shortening`
- `externalRotation`
- `immobilised`
- `immobilisationType`
- `immobilisationOther`
- `painScore`
- `pedalPulsePresent`
- `pedalPulseLog[]`
  - `value`
  - `timestamp`
- `prePainExemptions`
- `postPainExemptions`
- `analgesiaExemptions`

### Hypoglycaemia

- `interventions[]`
- `exceptions`
  - `preDone`
  - `preJustification`
  - `postDone`
  - `postJustification`
  - `noTreatment`
  - `noTreatJustification`
- `referral`
  - `referred`
  - `pathwayName`
  - `destination`
  - `referralDate`
- `generalComments`

### OOHCA / ROLE

- `arrest`
  - `timeCollapse`
  - `bystanderCPR`
  - `timeCPR`
  - `witnessed`
  - `timeWitnessed`
- `presenting`
  - `time`
  - `rhythm`
  - `rhythmOther`
- `suspectedCause`
- `shocks[]`
  - `time`
  - `energy`
  - `rhythm`
  - `rhythmOther`
- `roscTime`
- `postROSC`
  - `pulseCheck`
  - `airway`
  - `oxygenation`
  - `hemodynamics`
  - `ecg`
  - `temperature`
- `reversibleCauses`
  - `hypoxia`
  - `hypovolaemia`
  - `hypothermia`
  - `electrolyte`
  - `tensionPneumo`
  - `tamponade`
  - `toxins`
  - `thrombosis`
- `role`
  - `injuries`
  - `injuryDetails`
  - `witnessName`
  - `witnessRole`
- `verification`
  - `decomposition`
  - `rigorMortis`
  - `hypostasis`
  - `asystole30s`
- `pronounce`
  - `time` (legacy mirror of `ceasedResuscitationAt` for older exports)
  - `ceasedResuscitationAt` — resuscitation ceased / TOR
  - `verificationOfDeathAt` — optional; verification of life extinct when distinct from TOR
  - `byWhom`
  - `seniorRole`
  - `justification`
- `notify`
  - `family`
  - `familyNotes`
  - `police`
  - `policeJobNumber`

### Stroke

- `timeOfOnset`
- `isTIA`
- `facialDroop`
- `armWeakness`
- `speechDifficulty`
- `fastExemptions[]`
- `preAlertExemptions[]`
- `fastCompletedAt`
- `ataxia`
- `vomiting`
- `vertigo`
- `visionImpairment`
- `avvvCompletedAt`
- `comments`

## Safeguarding Module

### Current frontend behavior

Safeguarding is currently local-storage-backed via `safeguardingStorageService.ts`.

There are two active screens:

- `/safeguarding`
- `/safeguarding/form/:id`

### List view requirements

UI needs:

- list all referrals
- sort by `updatedAt desc`
- create new referral
- delete referral
- continue/view draft/submitted referral
- show:
  - `reference`
  - `subjectName`
  - `subjectType`
  - `status`
  - `updatedAt`

### Referral envelope

```ts
interface SafeguardingReferral {
  id: string;
  reference: string;
  status: 'draft' | 'submitted' | 'closed';
  createdAt: string;
  updatedAt: string;
  subjectType: 'child' | 'adult' | '';
  data: { ... }
}
```

### Persisted safeguarding data currently stored

- `subjectName`
- `subjectDob`
- `subjectAddress`
- `subjectPhone`
- `nhsNumber`
- `parentGuardian`
- `parentPhone`
- `abuseTypes[]`
- `otherConcerns`
- `riskLevel`
- `immediateRisk`
- `policeInvolved`
- `policeReference`
- `referrerName`
- `referrerRole`
- `referrerOrganisation`
- `referrerPhone`
- `referrerEmail`
- `consent`
- `consentDetails`
- `actionsTaken`
- `additionalInfo`
- `activeStep`

### Full form surface actually shown in the UI

The live safeguarding form captures more than the stored draft model and backend should support the full set:

- `personType`
- `forename`
- `surname`
- `dob`
- `address`
- `nhsNumber`
- `ethnicity`
- `language`
- `interpreterNeeded`
- `parentName`
- `parentContact`
- `parentRelationship`
- `abuseTypes[]`
- `abuseTypeOther`
- `urgency`
- `concernDetails`
- `evidenceObserved`
- `disclosure`
- `perpetratorKnown`
- `perpetratorDetails`
- `otherAgenciesInvolved`
- `consentObtained`
- `consentRefusedReason`
- `personInformedOfReferral`
- `immediateActionsToken`
- `referringTo[]`
- `referrerName`
- `referrerRole`
- `referrerContact`
- `referralDateTime`
- `incidentCAD`

### Safeguarding backend that now needs to exist

The frontend currently has no real safeguarding API. It needs one.

Recommended base:

- `/plugin/safeguarding_module/api`

Required endpoints:

- `GET /referrals`
  - filters:
    - `status`
    - `subjectType`
    - `assignedTo`
    - `createdBy`
    - `search`
- `POST /referrals`
- `GET /referrals/{referralId}`
- `PUT /referrals/{referralId}`
- `POST /referrals/{referralId}/submit`
- `POST /referrals/{referralId}/close`
- `DELETE /referrals/{referralId}`
- `POST /referrals/{referralId}/attachments`
- `GET /referrals/{referralId}/attachments`
- `DELETE /referrals/{referralId}/attachments/{attachmentId}`

Useful extras:

- `POST /referrals/prefill-from-epcr`
- `GET /referrals/{referralId}/audit`

### Safeguarding offline capability is mandatory

Safeguarding must support the same persistent pending-sync model as EPCR.

Required safeguarding offline behaviors:

1. New referral can be created fully offline.
2. Step-by-step autosave must persist locally while offline.
3. Submitting a referral while offline must mark it `pending_submit`, not fail or lose data.
4. Closing a referral while offline must queue the close action.
5. Attachments added offline must be retained and uploaded later.
6. The referrals list must clearly show sync state for each referral.
7. Reopening the browser/app must restore all safeguarding drafts and queued submissions.

Required safeguarding fields at envelope level for sync:

- `syncStatus`
- `pendingOperations[]`
- `lastLocalSaveAt`
- `lastServerAckAt`
- `syncError`
- `version`

Recommended safeguarding endpoints to support resilient sync:

- `POST /referrals/sync`
- `POST /referrals/{referralId}/sync-submit`
- `POST /referrals/{referralId}/sync-close`

Or equivalent idempotent standard endpoints, as long as:

- repeated submits do not create duplicates
- repeated closes do not create duplicates
- draft updates can be replayed safely

### Important safeguarding integration

The EPCR quick-action menu can generate a safeguarding referral prefilled from patient info:

- patient forename
- surname
- dob
- address
- nhsNumber

Backend should support this linkage.

## Minor Injury Module

### Current frontend behavior

Minor Injury is an event-driven workflow, not a generic standalone form.

Clinician flow:

1. fetch assigned event
2. show event dashboard
3. show notices, stats, event details, contacts, analytics, documents
4. create a new report under that event
5. submit report
6. poll event status every 30 seconds and disable submissions if event closes

### Current clinician-facing endpoints already documented in code

Base:

- `/plugin/minor_injury_module/api`

Existing endpoints used or defined by frontend service:

- `GET /events/assigned?userId={userId}`
- `GET /events/{eventId}/stats?userId={userId}`
- `GET /events/{eventId}/status`
- `GET /events/{eventId}/notices`
- `POST /events/{eventId}/reports`
- `GET /events/{eventId}/analytics`
- `POST /events/{eventId}/documents`

### Minor injury report payload required by UI

- `submittedBy`
- `submittedAt`
- `eventName`
- `eventLocation`
- `incidentDateTime`
- `incidentDescription`
- `patientName`
- `patientDob`
- `patientContact`
- `patientAddress`
- `parentGuardian`
- `isMinor`
- `allergies`
- `medications`
- `relevantHistory`
- `tetanusStatus`
- `vitalsChecked`
- `pulse`
- `respRate`
- `bp`
- `spo2`
- `temp`
- `painScore`
- `injuryType`
- `bodyLocation`
- `presentingComplaint`
- `injuriesDescription`
- `treatmentProvided`
- `outcome`
- `adviceGiven`
- `followUpRequired`
- `followUpDetails`
- `patientSignature`
- `clinicianName`
- `clinicianRole`
- `completedDateTime`

### Minor Injury offline capability is mandatory

Minor Injury currently behaves as if submission may fall back locally, but this now needs to become a real, durable offline-sync workflow.

Required minor injury offline behaviors:

1. Assigned event details already fetched must remain available offline.
2. If connection drops after event selection, the clinician must still be able to finish and save the report.
3. Submitting a report offline must create a durable local report with `pending_submit`.
4. The report must survive refresh, crash, browser close, and restart.
5. When connectivity returns, queued reports must sync automatically.
6. Duplicate report creation must be prevented through idempotency keys.
7. If an event closes while the user is offline, already-saved pending reports must not be discarded.
8. If the server rejects a queued report because the event was closed, the system must preserve the report, surface the rejection, and allow supervisor review/export rather than deleting it.

Required minor injury record sync metadata:

- `localReportId`
- `serverReportId`
- `eventId`
- `syncStatus`
- `syncError`
- `submittedOffline`
- `idempotencyKey`
- `lastLocalSaveAt`
- `lastServerAckAt`

Recommended minor injury sync endpoints:

- `POST /events/{eventId}/reports`
  - must be idempotent using `idempotencyKey`
- `POST /events/{eventId}/reports/bulk-sync`
  - optional but strongly recommended
- `GET /events/{eventId}/reports/{reportId}/sync-status`

### Controlled vocabularies required

#### Injury type

- `Laceration / Cut`
- `Abrasion / Graze`
- `Blister`
- `Contusion / Bruise`
- `Sprain / Strain`
- `Suspected Fracture`
- `Burn / Scald`
- `Insect Bite / Sting`
- `Head Injury`
- `Eye Injury`
- `Allergic Reaction`
- `Sunburn / Heat Exposure`
- `Dehydration`
- `Nausea / Vomiting`
- `Chest Pain`
- `Difficulty Breathing`
- `Mental Health / Welfare`
- `Intoxication`
- `Other`

#### Body location

- `Head / Face`
- `Neck`
- `Upper Limb - Shoulder`
- `Upper Limb - Arm`
- `Upper Limb - Elbow`
- `Upper Limb - Forearm`
- `Upper Limb - Wrist`
- `Upper Limb - Hand / Fingers`
- `Chest`
- `Abdomen`
- `Back - Upper`
- `Back - Lower`
- `Pelvis / Hip`
- `Lower Limb - Thigh`
- `Lower Limb - Knee`
- `Lower Limb - Shin / Calf`
- `Lower Limb - Ankle`
- `Lower Limb - Foot / Toes`
- `Multiple Locations`
- `Not Applicable`

#### Outcome values used by UI

- `discharged`
- `discharged_gp`
- `referred_hospital`
- `refused_treatment`

## Event Manager: Required Backend Surface

This is the area that must be explicitly added and made functional.

The current UI only exposes the clinician-facing side, but the event dashboard clearly assumes an event-management backend exists behind it.

Recommended base:

- `/plugin/minor_injury_module/api/admin`

### Events CRUD

- `GET /admin/events`
- `POST /admin/events`
- `GET /admin/events/{eventId}`
- `PUT /admin/events/{eventId}`
- `POST /admin/events/{eventId}/open`
- `POST /admin/events/{eventId}/close`
- `POST /admin/events/{eventId}/complete`
- `DELETE /admin/events/{eventId}`

### Event object required

- `id`
- `name`
- `location`
- `startDate`
- `endDate`
- `status`
  - `active`
  - `upcoming`
  - `completed`
  - `closed`
- `assignedUsers[]`
- `userSubmissions`
- `totalSubmissions`
- `notices[]`
- `riskProfile`
  - `expectedAttendance`
  - `keyHazards[]`
  - `nearestAE`
  - `siteMapRef`
- `contacts`
  - `controlChannel`
  - `controlExt`
  - `medicalLeadCallSign`
- `documents[]`
- `analytics`

### User assignment management

- `GET /admin/events/{eventId}/assignments`
- `POST /admin/events/{eventId}/assignments`
- `DELETE /admin/events/{eventId}/assignments/{userId}`
- `GET /admin/users/eligible-for-events`

### Notices / alerts

- `GET /admin/events/{eventId}/notices`
- `POST /admin/events/{eventId}/notices`
- `PUT /admin/events/{eventId}/notices/{noticeId}`
- `DELETE /admin/events/{eventId}/notices/{noticeId}`

Notice shape:

- `id`
- `message`
- `severity`
  - `info`
  - `warning`
  - `error`
- `timestamp`
- `expiresAt`

### Event documents

- `GET /admin/events/{eventId}/documents`
- `POST /admin/events/{eventId}/documents`
- `DELETE /admin/events/{eventId}/documents/{documentId}`

Document shape:

- `id`
- `name`
- `type`
  - `medical_plan`
  - `site_map`
  - `protocol`
  - `other`
- `url`

### Report operations

- `GET /admin/events/{eventId}/reports`
- `GET /admin/events/{eventId}/reports/{reportId}`
- `PUT /admin/events/{eventId}/reports/{reportId}`
- `POST /admin/events/{eventId}/reports/{reportId}/void`
- `GET /admin/events/{eventId}/export`

### Analytics and stats

- `GET /admin/events/{eventId}/stats`
- `GET /admin/events/{eventId}/analytics`
- `GET /admin/events/{eventId}/timeline`

Analytics shape required by UI:

- `injuryTypes[]`
  - `type`
  - `count`
  - `percentage`
- `outcomes[]`
  - `outcome`
  - `count`
  - `percentage`
- `hourlyTrends[]`
  - `hour`
  - `count`
- `bodyLocations[]`
  - `location`
  - `count`
  - `percentage`
- `lastUpdated`

### Event manager implications for offline reports

The event manager must be designed to handle delayed-arrival reports safely.

That means:

- reports created while offline may arrive after the live event state changed
- the backend must retain a distinction between:
  - incident time
  - local completion time
  - server receipt time
- rejected reports must never disappear silently
- control/admin users need a queue or exception view for:
  - late-arriving reports
  - rejected-on-sync reports
  - conflicts requiring manual review

## Datasets and Settings

### Current app settings used by UI

- `billingEnabled`
- `serverAddress`
- `localIpAddress`

### Current dataset groups

- drug costs
- clinical indicators
- clinical options
- iv fluids
- versions metadata

### Recommended backend/config endpoints

- `GET /config/app-settings`
- `PUT /config/app-settings`
- `GET /datasets/versions`
- `GET /datasets/drugs`
- `PUT /datasets/drugs`
- `GET /datasets/clinical-options`
- `PUT /datasets/clinical-options`
- `GET /datasets/clinical-indicators`
- `PUT /datasets/clinical-indicators`
- `GET /datasets/iv-fluids`
- `PUT /datasets/iv-fluids`

## File / Media Handling Required

The frontend currently stores images and signatures as base64 in places. Backend redesign should replace that with proper file storage.

Needed file categories:

- safeguarding attachments
- event documents
- clinical handover signatures
- advice document front/back images
- circulation ECG images
- needs supporting images

Recommended endpoints:

- `POST /files`
- `GET /files/{fileId}`
- `DELETE /files/{fileId}`

Or per-domain attachment endpoints if preferred.

## Known Gaps Between Current UI and Current Backend

These are important because Cursor should design around the actual UI, not the current partial API.

1. Login is mocked and has no real backend.
2. Safeguarding has no real backend at all.
3. Several EPCR sections are local-only or inconsistently persisted.
4. Some components still dispatch legacy action names like `UPDATE_CASE`; backend redesign should not assume those paths are already stable.
5. Minor Injury clinician endpoints are partly documented, but event-manager admin endpoints are not yet implemented in the frontend.
6. Files/images/signatures are not normalized into backend media storage yet.
7. Case sections are loosely typed; backend should enforce stronger schema validation.

## Recommended Backend Redesign Principles

1. Keep EPCR as a sectioned document model, but validate each section with explicit schemas.
2. Preserve existing section names to minimize frontend breakage.
3. Add versioning/audit history for EPCR, safeguarding, and minor injury reports.
4. Support draft autosave and final submission separately.
5. Keep offline sync compatibility:
   - idempotent create/update
   - conflict-safe timestamps
   - last-write / merge strategy
6. Expose event manager endpoints separately from clinician endpoints.
7. Normalize files and signatures into attachment records rather than raw base64 blobs.
8. Add role-based access control for event control, clinical users, and safeguarding reviewers.
9. Treat offline durability as a patient-safety and safeguarding-safety requirement, not a convenience feature.
10. Never delete unsynced local records automatically.
11. Preserve a recoverable local copy until the server has positively acknowledged successful sync.
12. Design all submit and close flows to be idempotent and replay-safe.

## Minimum Backend Contract Cursor Should Build

If building from scratch, the minimum complete backend should include:

- auth
- user profile
- ping/health
- ePCR case CRUD + close + polling
- VITA/patient lookup proxy
- datasets/config endpoints
- safeguarding referral CRUD + submit + close + attachments
- minor injury clinician endpoints
- event manager admin endpoints
- analytics endpoints
- file storage endpoints
- sync-safe idempotency support across all write endpoints
- durable pending-sync support for EPCR, safeguarding, and minor injury
- conflict/error endpoints or payloads that let the client preserve and recover unsynced records

## Suggested API Grouping

- `/api/auth/*`
- `/api/users/*`
- `/api/health/*`
- `/api/epcr/*`
- `/api/safeguarding/*`
- `/api/minor-injury/*`
- `/api/event-manager/*`
- `/api/datasets/*`
- `/api/files/*`

If plugin paths must be preserved for legacy deployment, map them internally while still keeping the contract above.

## Final Implementation Priority

Priority order for backend work:

1. Real auth
2. EPCR cases and section persistence
3. Safeguarding API
4. Minor injury clinician workflow
5. Event manager admin workflow
6. Files/attachments/signatures
7. Datasets/admin configuration
8. Analytics and exports
