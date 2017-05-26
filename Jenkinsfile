/* Define the tasks to build the different ISOs: SMW, CLE, and SLEUpdates */ 
def build_isos = [:]

// Global variables allow passing variables between stages
CLE_build_ID = ''
SMW_build_ID = ''
SLEUpdate_build_ID = ''

build_isos["SMW_iso"] = {
  stage ("Build SMW_iso"){    
      SMW_build = build job: 'PackageSMW-local', parameters: [string(name: 'INSTALLER_BRANCH', value: 'master'), string(name: 'CLE_OBS_PROJ', value: 'cle:master:ari'), string(name: 'ISO_SOURCE_PREF', value: 'build'), string(name: 'SMW_BUILD_ID', value: 'latest'), string(name: 'PACKAGER_BRANCH', value: 'master'), booleanParam(name: 'RUN_TESTS', value: false), string(name: 'SMW_PIM_BRANCH', value: 'master')]
      SMW_build_ID = Integer.toString(SMW_build.getNumber())
  }
}

build_isos["CLE_iso"] = {
  stage ("Build CLE_iso"){    
      CLE_build = build job: 'PackageCLE-local', parameters: [string(name: 'INSTALLER_BRANCH', value: 'master'), string(name: 'CLE_OBS_PROJ', value: 'cle:master:ari'), string(name: 'ISO_SOURCE_PREF', value: 'build'), string(name: 'CLE_BUILD_ID', value: 'latest'), string(name: 'PACKAGER_BRANCH', value: 'master'), booleanParam(name: 'RUN_TESTS', value: false), string(name: 'CLE_PIM_BRANCH', value: 'master')]
      CLE_build_ID = Integer.toString(CLE_build.getNumber())
  }
}

build_isos["SLEUpdate_iso"] = {
  stage ("Build SLEUpdate_iso"){    
      SLEUpdate_build = build job: 'ArchiveSLEUpdate', parameters: [string(name: 'CI_BRANCH', value: 'master'), booleanParam(name: 'USE_ARCHIVED_PACKAGESLEUPDATES_LOCAL', value: false), booleanParam(name: 'USE_PREBUILT_SLEUPDATE_ISO', value: true), string(name: 'CLE_PIM_RELEASE_BRANCH', value: 'master')]
      // Environment variables also allow passing variables
      env.SLEUpdate_build_ID = Integer.toString(SLEUpdate_build.getNumber())
  }
}

def get_built_isos = [:]

/* I am passing the *_build_ID variables via two different methods below,
   as global variables and as environment variables.
   This is done for illustration.
   */

get_built_isos["CLE_iso"] = {
  stage("Copy newly built CLE artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'PackageCLE-local', 
        selector: [$class: 'SpecificBuildSelector', buildNumber: CLE_build_ID], 
        target: './.'])
  }
}

get_built_isos["SMW_iso"] = {
  stage("Copy newly built SMW artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'PackageSMW-local', 
        selector: [$class: 'SpecificBuildSelector', buildNumber: SMW_build_ID],
        target: './.'])
  }
}

get_built_isos["ArchiveSLEUpdate_iso"] = {
  stage("Copy newly built ArchiveSLEUpdate artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'ArchiveSLEUpdate', 
        selector: [$class: 'SpecificBuildSelector', buildNumber: env.SLEUpdate_build_ID], 
        target: './.'])
  }  
}

def get_existing_isos = [:]

get_existing_isos["CLE_iso"] = {
  stage("Copy last successful CLE artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'PackageCLE-local', 
        selector: [$class: 'StatusBuildSelector', stable: false], 
        target: './.'])
  }
}

get_existing_isos["SMW_iso"] = {
  stage("Copy last successful SMW artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'PackageSMW-local', 
        selector: [$class: 'StatusBuildSelector', stable: false], 
        target: './.'])
  }
}

get_existing_isos["ArchiveSLEUpdate_iso"] = {
  stage("Copy last successful ArchiveSLEUpdate artifacts") {
    step([$class: 'CopyArtifact', 
        filter: '', 
        fingerprintArtifacts: true, 
        projectName: 'ArchiveSLEUpdate', 
        selector: [$class: 'StatusBuildSelector', stable: false], 
        target: './.'])
  }  
}

// Main 

node('romans') {

  stage ('Set-up') {
    hipchatSend(color: 'GREEN', notify: true, message: "firebomb-integration CI is starting.", room: 'cray mgmt sw - ci')
    echo "BUILD-ID: $BUILD_ID"
    echo "WORKSPACE: $WORKSPACE" 
    sh "mkdir -p $WORKSPACE/$BUILD_ID"
    sh "mkdir -p $WORKSPACE/$BUILD_ID/isos"
    sh "mkdir -p $WORKSPACE/$BUILD_ID/ci"
  }

  
  	if (! params.SKIP_ISOS) {
  		// Trigger the build of the ISOs
    	parallel build_isos

		/* TODO: It would be better to conditionalize the selector
			and pass it into the CopyArtifact step than to have
			get_built_isos and get_existing_isos*/
    	dir("$WORKSPACE/$BUILD_ID/isos") {
        	parallel get_built_isos
    	}    
  	} else {
  		// Did not build the ISOs.  Just get the latest ones.
    	dir("$WORKSPACE/$BUILD_ID/isos") {
        	parallel get_existing_isos
    	}
    }

  	stage ('Notify end user') {    
    	hipchatSend color: 'YELLOW', message: '\'$JOB_NAME #$BUILD_NUMBER $STATUS ($CHANGES_OR_CAUSE) (<a href="$URL">Open</a>)\'', notify: true, room: 'cray mgmt sw - ci', sendAs: '', server: 'cray.hipchat.com', token: '5489bfeec3784370fc2c7ae3166628', v2enabled: true
  	}
  	
    dir("$WORKSPACE/$BUILD_ID/ci") {
        stage ('Checkout CI tools') {
            git changelog: false, 
            	credentialsId: 'jenkins', 
            	poll: false, 
            	url: 'ssh://git@stash.us.cray.com:7999/~jasons/ci.git', 
            	branch: 'integration'
            sh 'cp src/jobs/SMWCLEUpdate.py fabfile.py'
        }
    }
    
    dir("$WORKSPACE/$BUILD_ID/") {
      stage('Stage ISO into CI tools directory') {
          sh 'find -name "*.iso" -exec cp \'{}\' ./ci/ \\;'
      }
    }

    dir("$WORKSPACE/$BUILD_ID/ci") {
        try {
    	    stage ('Update MOTD') {
    	        sh 'fab update_motd:updating=True'
    	    }
    	    stage ('update_smw_cle') {
    	      sh 'fab update_smw_cle'
    	    }
    	    stage ('tell_nims_things') {
    	      sh 'fab tell_nims_things'
    	    }
    	    
    	    stage ('bounce_and_boot_cle') {
    	      sh 'fab bounce_and_boot_cle'
    	    }
    	    stage ('smoke_test()') {
    	      sh 'fab smoke_test'
    	    }
    	    stage ('update_motd') {
    	      sh 'fab update_motd'
    	    }
    	
    	    if (params.WITH_NETROOT) {
    	        stage ('build_and_push_netroot_images') {
    	          sh 'fab build_and_push_netroot_images'
    	        }
    	        stage ('sync_bootnode_disks') {
    	          sh 'fab sync_bootnode_disks'
    	        }
    	        stage ('reboot_netroot') {
    	          sh 'fab reboot_netroot'
    	        }
    	    }
    	    
    	    currentBuild.result = 'SUCCESS'
    	} catch(ex) {
    		stage ('Update MOTD') {
    			echo "Printing exception as a string."
    			println(ex.toString());
    			echo "Prining exception message."
             	println(ex.getMessage());
             	println(ex.getStackTrace());
    	        sh 'fab update_motd:failed=True'
    	        currentBuild.result = 'FAILURE'
    	        hipchatSend(color: 'YELLOW', notify: true, message: "firebomb-integration CI failed.  There was much weeping and gnashing of teeth.", room: 'cray mgmt sw - ci')
    	    }
    	} finally {
    		stage('Send status updates') {
				step([$class: 'Mailer', notifyEveryUnstableBuild: true, recipients: "${EMAIL_LIST}", sendToIndividuals: true])
				hipchatSend(color: 'YELLOW', notify: true, message: "firebomb-integration CI is done.", room: 'cray mgmt sw - ci')
		}  	
    }
  }  
}
