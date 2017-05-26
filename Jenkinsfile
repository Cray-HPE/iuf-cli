def tasks = [:]

tasks["SMW_iso"] = {
  stage ("SMW_iso"){    
    node('master') {  
      build job: 'Create-SMW-Media', parameters: [string(name: 'INSTALLER_BRANCH', value: 'master'), string(name: 'CLE_OBS_PROJ', value: 'cle:master:ari'), string(name: 'ISO_SOURCE_PREF', value: 'build'), string(name: 'SMW_BUILD_ID', value: 'latest'), string(name: 'PACKAGER_BRANCH', value: 'master'), booleanParam(name: 'RUN_TESTS', value: false), string(name: 'SMW_PIM_BRANCH', value: 'master')]
    }
  }
}

tasks["CLE_iso"] = {
  stage ("CLE_iso"){    
    node('master') {  
      build job: 'Create-CLE-Media', parameters: [string(name: 'INSTALLER_BRANCH', value: 'master'), string(name: 'CLE_OBS_PROJ', value: 'cle:master:ari'), string(name: 'ISO_SOURCE_PREF', value: 'build'), string(name: 'CLE_BUILD_ID', value: 'latest'), string(name: 'PACKAGER_BRANCH', value: 'master'), booleanParam(name: 'RUN_TESTS', value: false), string(name: 'CLE_PIM_BRANCH', value: 'master')]
    }
  }
}

tasks["SLEUpdates_iso"] = {
  stage ("SLEUpdates_iso"){    
    node('master') {  
      build job: 'ArchiveSLEUpdate', parameters: [string(name: 'CI_BRANCH', value: 'master'), booleanParam(name: 'USE_ARCHIVED_PACKAGESLEUPDATES_LOCAL', value: false), booleanParam(name: 'USE_PREBUILT_SLEUPDATE_ISO', value: true), string(name: 'CLE_PIM_RELEASE_BRANCH', value: 'master')]
    }
  }
}

parallel tasks