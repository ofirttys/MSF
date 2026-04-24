
// ============================================================================
// DEBUG TIMING (Performance Monitoring)
// ============================================================================
// To enable: Run with "python app.py --debug" or type enableDebugTiming() in console

var DEBUG_TIMING = false;

window.enableDebugTiming = function() {
    DEBUG_TIMING = true;
    console.log('⏱️ Performance monitoring enabled');
};

window.disableDebugTiming = function() {
    DEBUG_TIMING = false;
    console.log('⏱️ Performance monitoring disabled');
};

window.checkDebugStatus = function() {
    console.log('⏱️ Performance monitoring:', DEBUG_TIMING ? 'enabled' : 'disabled');
};

        // ============================================================================
        // CONFIGURATION - MODIFY THIS SECTION AT THE HOSPITAL
        // ============================================================================
        
		var ROOT_PATH = "DB\\";
        
        var DATABASE_FILE_PATH = ROOT_PATH + "michaeli-clinic.json";
        var CLINIC_DAYS_FILE_PATH = ROOT_PATH + "clinic-days.json";
        var LOCK_FILE_PATH = ROOT_PATH + "michaeli-clinic.lock";
        var ACTION_ITEMS_FILE_PATH = ROOT_PATH + "action-items.json";
        var EMAIL_TEMPLATES_FILE_PATH = ROOT_PATH + "email-templates.json";
        var PENDING_EMAILS_FILE_PATH = ROOT_PATH + "pending-emails.json";
		var PORTAL_USERS_FILE_PATH = ROOT_PATH + "Patient Portal Users.csv";
		
		var EMAIL_FROM_ADDRESS = "dr.michaelisoffice@sinaihealth.ca"; // Leave empty to use default account
		
        // Lock file settings
        var LOCK_STALE_HOURS = 1; // Lock files older than this are considered stale/orphaned
        
        // Credentials - multiple users supported
        var VALID_USERS = {
            "admin": "5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da",
            "jennia": "5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da"
        };
        
        // DEBUG MODE: Set to true to see password hashes (for changing password)
        // After getting the hash, set back to false!
        var DEBUG_MODE = false;
        
        // Read-only mode flag
        var isReadOnly = false;
        var currentUser = "";
        var lockOwner = "";
        var isAdmin = false;  // Track if current user is admin
        
        // Action items data
        var actionItems = {
            activeTab: "all",
            appointment: [],
            general: [],
            phone: [],
            email: []
        };
        
        // ============================================================================
        // END CONFIGURATION
        // ============================================================================

        // ============================================================================
        // DEBUG TIMING SYSTEM
        // ============================================================================
        
        var timingStack = [];
        
        function startTiming(operationName) {
            if (!DEBUG_TIMING) return;
            var timestamp = performance.now();
            timingStack.push({
                name: operationName,
                start: timestamp
            });
            console.log(`⏱️ [TIMING] START: ${operationName} at ${timestamp.toFixed(2)}ms`);
        }
        
        function endTiming(operationName) {
            if (!DEBUG_TIMING) return;
            var timestamp = performance.now();
            
            // Find matching start in stack
            for (var i = timingStack.length - 1; i >= 0; i--) {
                if (timingStack[i].name === operationName) {
                    var duration = timestamp - timingStack[i].start;
                    console.log(`✅ [TIMING] END: ${operationName} - Duration: ${duration.toFixed(2)}ms (${(duration/1000).toFixed(2)}s)`);
                    timingStack.splice(i, 1);
                    return duration;
                }
            }
            
            console.log(`❌ [TIMING] END: ${operationName} at ${timestamp.toFixed(2)}ms (no matching start found)`);
        }
        
        function logTiming(message) {
            if (!DEBUG_TIMING) return;
            var timestamp = performance.now();
            console.log(`📍 [TIMING] ${message} at ${timestamp.toFixed(2)}ms`);
        }
        
        // Check if running in debug mode
        async function checkDebugMode() {
            try {
                var args = await eel.get_command_line_args()();
                
                if (args && args.includes('--debug')) {
                    DEBUG_TIMING = true;
                    console.log('⏱️ Performance monitoring enabled (--debug flag)');
                    return;
                }
            } catch (error) {
                // Silently fail - not critical
            }
            
            // Fallback: check URL parameter
            var urlParams = new URLSearchParams(window.location.search);
            if (urlParams.get('debug') === 'true') {
                DEBUG_TIMING = true;
                console.log('⏱️ Performance monitoring enabled (URL parameter)');
                return;
            }
            
            // Debug mode disabled - console stays clean
        }
        
        // ============================================================================
        // GLOBAL VARIABLES
        // ============================================================================

		function getTodayLocalDate() {
			var d = new Date();
			var year = d.getFullYear();
			var month = d.getMonth() + 1;
			var day = d.getDate();
			return year + "-" + (month < 10 ? "0" : "") + month + "-" + (day < 10 ? "0" : "") + day;
		}

        // State machine configuration
        var STATES = {
            WAITING_FIRST_APPT_SCHEDULE: {
                label: 'Waiting for First Appointment to be Scheduled',
                shortLabel: 'Waiting - First Appointment',
                color: '#e74c3c',
                next: 'WAITING_FIRST_APPT'
            },
            WAITING_FIRST_APPT: {
                label: 'Waiting for First Appointment',
                shortLabel: 'Waiting - First Appointment',
                color: '#f39c12',
                next: 'WAITING_APPT_SUMMARY'
            },
            WAITING_APPT_SUMMARY: {
                label: 'Waiting for Appointment Summary',
                shortLabel: 'Waiting - Summary',
                color: '#9b59b6',
                next: 'WAITING_NEXT_APPT_SCHEDULE'
            },
            WAITING_NEXT_APPT_SCHEDULE: {
                label: 'Waiting for Next Appointment to be Scheduled',
                shortLabel: 'Waiting - Next Appointment Schedule',
                color: '#3498db',
                next: 'WAITING_NEXT_APPT'
            },
            WAITING_NEXT_APPT: {
                label: 'Waiting for Next Appointment',
                shortLabel: 'Waiting - Next Appointment',
                color: '#27ae60',
                next: 'WAITING_APPT_SUMMARY'
            },
            PREGNANT: {
                label: 'Pregnant',
                shortLabel: 'Pregnant',
                color: '#e91e63',
                next: 'WAITING_NEXT_APPT_SCHEDULE'
            },
            INACTIVE: {
                label: 'Inactive',
                shortLabel: 'Inactive',
                color: '#95a5a6',
                next: 'WAITING_NEXT_APPT_SCHEDULE'
            },
            OVERDUE_APPOINTMENT: {
                label: 'Overdue - Appointment Not Completed',
                shortLabel: 'Overdue',
                color: '#c0392b',
                next: null
            }
        };

        // Data storage
        var patients = [];
        var clinicDays = {}; // Clinic days data: { "YYYY-MM-DD": { vaughan: true, virtual: true, ... } }
        var currentViewDate = new Date();
        var currentEditingPatient = null;
        var currentTransitionPatient = null;
        var currentEditingApptPatient = null;
        var isLoggedIn = false;
        var currentFilter = []; // Array to hold multiple active filters
        var currentSortMode = 'name'; // Sort mode: 'name', 'appt-new', 'appt-old'
        var autoSaveInterval = null;

        // Helper function to merge objects (IE doesn't support Object.assign)
        function mergeObjects(target) {
            for (var i = 1; i < arguments.length; i++) {
                var source = arguments[i];
                if (source) {
                    for (var key in source) {
                        if (source.hasOwnProperty(key)) {
                            target[key] = source[key];
                        }
                    }
                }
            }
            return target;
        }

        // Helper function to build full name from parts
        function buildFullName(first, middle, last) {
            var parts = [];
            if (first) parts.push(first);
            if (middle) parts.push(middle);
            if (last) parts.push(last.toUpperCase());
            return parts.join(' ');
        }

		// Helper function to format name with alias for display
        // Uses new fields if available, otherwise falls back to fullName
        function formatNameWithAlias(fullName, alias, firstName, middleName, lastName) {
            // If we have new name fields, build "First (alias) Middle Last"
            if (firstName || lastName) {
                var parts = [];
                if (firstName) {
                    if (alias && alias.trim()) {
                        parts.push(firstName + ' (' + alias.trim() + ')');
                    } else {
                        parts.push(firstName);
                    }
                }
                if (middleName) parts.push(middleName);
                if (lastName) parts.push(lastName);
                return parts.join(' ');
            }
            // Fall back to fullName with alias appended
            if (!fullName) return '';
            if (alias && alias.trim()) {
                return fullName + ' (' + alias.trim() + ')';
            }
            return fullName;
        }

        // Simple but effective hash function (works in IE/HTA)
        function hashPassword(str) {
            var hash = 0;
            var salt = 'michaeli_clinic_2025';
            var combined = str + salt;
            
            for (var i = 0; i < combined.length; i++) {
                var char = combined.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash; // Convert to 32-bit integer
            }
            
            // Convert to positive hex string with padding
            var hexHash = (hash >>> 0).toString(16);
            while (hexHash.length < 8) {
                hexHash = '0' + hexHash;
            }
            
            // Extend to 64 characters for better security appearance
            var extended = hexHash;
            for (var j = 0; j < 7; j++) {
                extended += hashPassword.simpleHash((hexHash + j).toString());
            }
            
            return extended.substring(0, 64);
        }
        
        // Helper function for hash extension
        hashPassword.simpleHash = function(s) {
            var h = 0;
            for (var i = 0; i < s.length; i++) {
                h = ((h << 5) - h) + s.charCodeAt(i);
                h = h & h;
            }
            return ((h >>> 0).toString(16)).substring(0, 8);
        };

        // Login handling
        async function handleLogin(event) {
            event.preventDefault();
            
            var username = document.getElementById('loginUsername').value.toLowerCase();
            var password = document.getElementById('loginPassword').value;
            var errorDiv = document.getElementById('loginError');
            
            // Call backend login
            try {
                var result = await eel.login(username, password)();
                
                if (result.status === 'error') {
                    errorDiv.textContent = result.message || 'Invalid username or password';
                    errorDiv.style.display = 'block';
                    return false;
                }
                
                // Login successful
                currentUser = result.username;
                isAdmin = result.isAdmin;
                
                // Register session (prevent duplicate logins)
                var sessionResult = await eel.register_session(username)();
                if (!sessionResult.success) {
                    // Session blocked - offer force logout
                    var forceLogout = confirm(
                        sessionResult.error + '\n\n' +
                        'This can happen if you closed the app without logging out.\n\n' +
                        'Click OK to force logout the previous session and login now.\n' +
                        'Click Cancel to wait for the session to expire.'
                    );
                    
                    if (forceLogout) {
                        // Force logout the previous session
                        var forceResult = await eel.force_logout_user(username)();
                        if (forceResult.success) {
                            // Try registering session again
                            sessionResult = await eel.register_session(username)();
                            if (!sessionResult.success) {
                                errorDiv.textContent = 'Failed to create session after force logout';
                                errorDiv.style.display = 'block';
                                return false;
                            }
                        } else {
                            errorDiv.textContent = forceResult.error || 'Failed to force logout';
                            errorDiv.style.display = 'block';
                            return false;
                        }
                    } else {
                        // User chose to wait
                        errorDiv.textContent = sessionResult.error;
                        errorDiv.style.display = 'block';
                        return false;
                    }
                }
                
                // Check for lock file
                var lockStatus = checkLockFile();
                
                if (lockStatus.locked && !lockStatus.stale) {
                    // Database is locked by another user
                    var lockTime = new Date(lockStatus.timestamp);
                    var lockTimeStr = lockTime.toLocaleDateString('en-US', {timeZone: 'America/Toronto'}) + ' at ' + lockTime.toLocaleTimeString('en-US', {timeZone: 'America/Toronto'});
                    
                    var choice = confirm(
                        'Database In Use\n\n' +
                        'The database is currently being edited by: ' + lockStatus.user + '\n' +
                        'Since: ' + lockTimeStr + '\n\n' +
                        'Click OK to open in Read-Only mode (you can view but not edit).\n' +
                        'Click Cancel to abort login.'
                    );
                    
                    if (choice) {
                        isReadOnly = true;
                        lockOwner = lockStatus.user;
                    } else {
                        return false;
                    }
                } else {
                    // No lock or stale lock - create new lock
                    if (lockStatus.stale) {
                        // Inform user we're breaking stale lock
                        showErrorModal('Note: A stale lock from ' + lockStatus.user + ' was found and has been cleared.');
                    }
                    createLockFile(username);
                    isReadOnly = false;
                }
                
                isLoggedIn = true;
                currentUser = username;
                
                document.getElementById('loginScreen').style.display = 'none';
                document.getElementById('mainApp').style.display = 'block';
                
                // Display username in database bar
                var userInfo = document.getElementById('currentUserInfo');
                if (userInfo) {
                    userInfo.textContent = username;
                }
                
                // Show/hide Users button based on admin status
                var usersBtn = document.getElementById('usersBtn');
                if (usersBtn) {
                    usersBtn.style.display = isAdmin ? 'inline-block' : 'none';
                }
                
                await initializeApp();
                
                // Start session heartbeat (every 5 minutes)
                setInterval(async function() {
                    await eel.update_session_heartbeat(username)();
                    updateActiveUsers();
                }, 5 * 60 * 1000);
                
                // Update active users immediately and every minute
                updateActiveUsers();
                setInterval(updateActiveUsers, 60 * 1000);
                
            } catch (error) {
                console.error('Login error:', error);
                errorDiv.textContent = 'An error occurred during login';
                errorDiv.style.display = 'block';
            }
            
            return false;
        }

        async function logout() {
            // Unregister session
            if (currentUser) {
                await eel.unregister_session(currentUser)();
            }
            
            // Clear auto-save timer
            if (autoSaveInterval) {
                clearInterval(autoSaveInterval);
                autoSaveInterval = null;
            }
            
            // Delete lock file if we own it
            if (!isReadOnly) {
                deleteLockFile();
            }
            
            isLoggedIn = false;
            isReadOnly = false;
            currentUser = "";
            document.getElementById('mainApp').style.display = 'none';
            document.getElementById('loginScreen').style.display = 'flex';
            document.getElementById('loginUsername').value = '';
            document.getElementById('loginPassword').value = '';
            document.getElementById('loginError').style.display = 'none';
            
            // Hide read-only banner if shown
            var banner = document.getElementById('readOnlyBanner');
            if (banner) {
                banner.style.display = 'none';
            }
        }

        // ============================================================================
        // USER SETTINGS DROPDOWN
        // ============================================================================
        
        function toggleUserSettingsDropdown() {
            var dropdown = document.getElementById('userSettingsDropdown');
            if (dropdown.style.display === 'none' || dropdown.style.display === '') {
                dropdown.style.display = 'block';
                
                // Show/hide Manage Users option based on admin status
                var manageUsersOption = document.getElementById('manageUsersOption');
                if (manageUsersOption) {
                    manageUsersOption.style.display = isAdmin ? 'block' : 'none';
                }
            } else {
                dropdown.style.display = 'none';
            }
        }
        
        // Close dropdown when clicking outside
        document.addEventListener('click', function(event) {
            var dropdown = document.getElementById('userSettingsDropdown');
            var btn = document.getElementById('userSettingsBtn');
            
            if (dropdown && btn) {
                if (!btn.contains(event.target) && !dropdown.contains(event.target)) {
                    dropdown.style.display = 'none';
                }
            }
        });
        
        // ============================================================================
        // CHANGE PASSWORD
        // ============================================================================
        
        function openChangePasswordModal() {
            // Clear form
            document.getElementById('currentPassword').value = '';
            document.getElementById('newPassword').value = '';
            document.getElementById('confirmPassword').value = '';
            
            // Hide messages
            document.getElementById('changePasswordError').style.display = 'none';
            document.getElementById('changePasswordSuccess').style.display = 'none';
            
            // Open modal
            document.getElementById('changePasswordModal').classList.add('active');
        }
        
        async function submitPasswordChange() {
            var currentPwd = document.getElementById('currentPassword').value;
            var newPwd = document.getElementById('newPassword').value;
            var confirmPwd = document.getElementById('confirmPassword').value;
            
            var errorDiv = document.getElementById('changePasswordError');
            var successDiv = document.getElementById('changePasswordSuccess');
            
            // Hide previous messages
            errorDiv.style.display = 'none';
            successDiv.style.display = 'none';
            
            // Validation
            if (!currentPwd || !newPwd || !confirmPwd) {
                errorDiv.textContent = 'Please fill in all fields';
                errorDiv.style.display = 'block';
                return;
            }
            
            if (newPwd !== confirmPwd) {
                errorDiv.textContent = 'New passwords do not match';
                errorDiv.style.display = 'block';
                return;
            }
            
            if (newPwd.length < 8) {
                errorDiv.textContent = 'Password must be at least 8 characters';
                errorDiv.style.display = 'block';
                return;
            }
            
            var hasLower = /[a-z]/.test(newPwd);
            var hasUpper = /[A-Z]/.test(newPwd);
            var hasDigit = /[0-9]/.test(newPwd);
            
            if (!hasLower || !hasUpper || !hasDigit) {
                errorDiv.textContent = 'Password must contain uppercase, lowercase, and numbers';
                errorDiv.style.display = 'block';
                return;
            }
            
            // Call backend
            try {
                var result = await eel.change_password(currentPwd, newPwd)();
                
                if (result && result.status === 'success') {
                    successDiv.textContent = result.message;
                    successDiv.style.display = 'block';
                    errorDiv.style.display = 'none';
                    
                    // Clear form
                    document.getElementById('currentPassword').value = '';
                    document.getElementById('newPassword').value = '';
                    document.getElementById('confirmPassword').value = '';
                    
                    // Auto-close after 2 seconds
                    setTimeout(function() {
                        successDiv.style.display = 'none';
                        closeModal('changePasswordModal');
                    }, 2000);
                } else {
                    errorDiv.textContent = result ? result.message : 'Unknown error occurred';
                    errorDiv.style.display = 'block';
                    successDiv.style.display = 'none';
                }
            } catch (error) {
                errorDiv.textContent = 'Error changing password: ' + error;
                errorDiv.style.display = 'block';
                successDiv.style.display = 'none';
            }
        }
        
        async function updateActiveUsers() {
            try {
                var activeUsers = await eel.get_active_users()();
                var otherUsers = activeUsers.filter(function(u) { return u.username !== currentUser; });
                
                var userInfo = document.getElementById('otherUsersInfo');
                if (userInfo) {
                    if (otherUsers.length === 0) {
                        userInfo.textContent = '';
                    } else {
                        var names = otherUsers.map(function(u) { 
                            return u.username + ' (' + u.minutes_ago + 'm ago)';
                        }).join(', ');
                        userInfo.textContent = 'Also active: ' + names;
                    }
                }
            } catch (error) {
                console.error('Error updating active users:', error);
            }
        }
        
        // Lock file functions
        function checkLockFile() {
            // Lock file system disabled in Eel version - backend handles concurrency
            return { locked: false, stale: false, user: null, timestamp: null };
        }
        
        function createLockFile(username) {
            // Lock file system disabled in Eel version - backend handles concurrency
        }
        
        function deleteLockFile() {
            // Lock file system disabled in Eel version - backend handles concurrency
        }
        
        
        function refreshLockFile() {
            if (!isReadOnly && currentUser) {
                createLockFile(currentUser);
            }
        }

        // Initialize application
        async function initializeApp() {
            // Check if debug timing is enabled
            await checkDebugMode();
            
            document.getElementById('filePathDisplay').textContent = 'Backend Database (SQLite)';
            
            // loadDatabase now handles showing mainApp and rendering UI
            await loadDatabase();
            
            // Apply read-only mode if needed
            applyReadOnlyMode();
            
            // Auto-transition patients with past appointments (only if not read-only)
            if (!isReadOnly) {
                autoTransitionPastAppointments();
            }
            
            // Lock file refresh timer (5 minutes) - only if not read-only
            if (autoSaveInterval) {
                clearInterval(autoSaveInterval);
            }
            if (!isReadOnly) {
                autoSaveInterval = setInterval(function() {
                    // Refresh lock file to keep it fresh (DISABLED - not needed with backend SQLite locking)
                    // refreshLockFile();
                }, 5 * 60 * 1000); // 5 minutes
            }
            
            // Smart auto-refresh: Only reload if database actually changed
            var lastRefreshTimestamp = null;
            var isManualOperationInProgress = false;
            
            // Reusable smart refresh function
            async function doSmartRefresh(reason) {
                if (isManualOperationInProgress) {
                    if (DEBUG_TIMING) {
                        console.log(`Skipping smart refresh (${reason}) - manual operation in progress`);
                    }
                    return false;
                }
                
                try {
                    var currentTimestamp = await eel.get_last_modified_timestamp()();
                    
                    if (lastRefreshTimestamp === null) {
                        lastRefreshTimestamp = currentTimestamp;
                        return false;
                    }
                    
                    if (currentTimestamp !== lastRefreshTimestamp) {
                        startTiming('smart_refresh_' + reason);
                        
                        var changedIDs = await eel.get_changed_patients_since(lastRefreshTimestamp)();
                        
                        if (changedIDs.length > 0) {
                            // Only log when changes actually found
                            console.log(`📊 Smart refresh: ${changedIDs.length} patient(s) updated by another user`);
                            
                            for (var i = 0; i < changedIDs.length; i++) {
                                var id = changedIDs[i];
                                var updatedPatient = await eel.get_patient(id)();
                                
                                var found = false;
                                for (var j = 0; j < patients.length; j++) {
                                    if (patients[j].patientID === id) {
                                        patients[j] = updatedPatient;
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if (!found) {
                                    patients.push(updatedPatient);
                                    console.log('📝 New patient added:', updatedPatient.patientName);
                                }
                            }
                            
                            // Re-render UI with updated patients
                            renderPatientList();
                            await renderAppointments();
                            updateStatusCounts();
                        }
                        
                        endTiming('smart_refresh_' + reason);
                        lastRefreshTimestamp = currentTimestamp;
                        return true;
                    }
                    
                    // No changes - don't log anything (keep console clean!)
                    return false;
                } catch (error) {
                    console.error('Error in smart refresh:', error);
                    return false;
                }
            }
            
            // Background auto-refresh (every 15 seconds)
            setInterval(async function() {
                await doSmartRefresh('auto_refresh_timer');
            }, 15 * 1000); // Check every 15 seconds
            
            // Check and create daily backup (first use each day)
            if (!isReadOnly) {
                checkAndCreateDailyBackup();
            }
        }
        
        // Apply read-only mode restrictions
        function applyReadOnlyMode() {
            var banner = document.getElementById('readOnlyBanner');
            var saveBtn = document.getElementById('saveButton');
            var addPatientBtn = document.getElementById('addPatientBtn');
            
            if (isReadOnly) {
                // Show banner
                if (banner) {
                    banner.style.display = 'block';
                    document.getElementById('lockOwnerName').textContent = lockOwner;
                }
                
                // Disable save button
                if (saveBtn) {
                    saveBtn.disabled = true;
                    saveBtn.style.opacity = '0.5';
                    saveBtn.style.cursor = 'not-allowed';
                    saveBtn.title = 'Read-only mode - cannot save';
                }
                
                // Disable add patient button
                if (addPatientBtn) {
                    addPatientBtn.disabled = true;
                    addPatientBtn.style.opacity = '0.5';
                    addPatientBtn.style.cursor = 'not-allowed';
                    addPatientBtn.title = 'Read-only mode - cannot add patients';
                }
            } else {
                // Hide banner
                if (banner) {
                    banner.style.display = 'none';
                }
                
                // Enable buttons
                if (saveBtn) {
                    saveBtn.disabled = false;
                    saveBtn.style.opacity = '1';
                    saveBtn.style.cursor = 'pointer';
                    saveBtn.title = '';
                }
                
                if (addPatientBtn) {
                    addPatientBtn.disabled = false;
                    addPatientBtn.style.opacity = '1';
                    addPatientBtn.style.cursor = 'pointer';
                    addPatientBtn.title = '';
                }
            }
        }

        // Auto-transition patients with past appointments
        function autoTransitionPastAppointments() {
			var today = new Date();
			var oneWeekAgo = new Date();
			oneWeekAgo.setDate(today.getDate() - 7);
			var year = oneWeekAgo.getFullYear();
			var month = oneWeekAgo.getMonth() + 1;
			var day = oneWeekAgo.getDate();
			var oneWeekAgoStr = year + "-" + (month < 10 ? "0" : "") + month + "-" + (day < 10 ? "0" : "") + day;

			var changed = false;
            
            for (var i = 0; i < patients.length; i++) {
                var patient = patients[i];
                
                // Check if patient is waiting for appointment and date has passed
                if ((patient.currentState === 'WAITING_FIRST_APPT' || patient.currentState === 'WAITING_NEXT_APPT') 
                    && patient.nextAppointment && patient.nextAppointment <= oneWeekAgo) {
                    
                    // Move to history
                    patients[i].appointmentHistory.push({
                        date: patient.nextAppointment,
                        time: patient.appointmentTime || '',
                        summary: 'Auto-transitioned - appointment date passed',
                        timestamp: new Date().toISOString()
                    });
                    
                    // Update state
                    patients[i].currentState = 'WAITING_APPT_SUMMARY';
                    patients[i].stateHistory.push({
                        state: 'WAITING_APPT_SUMMARY',
                        timestamp: new Date().toISOString()
                    });
                    patients[i].nextAppointment = null;
                    patients[i].appointmentTime = null;
                    
                    changed = true;
                }
            }
            
            // TODO: If changed, should save to backend via updatePatientStateWithSave()
        }
        
        // Load remaining patients in ONE query (simple 2-step approach)
        async function loadRemainingPatientsInBackground(currentOffset, totalPatients) {
            startTiming('background_patient_load');
            
            var remaining = totalPatients - currentOffset;
            console.log(`Background loading: ${remaining} remaining patients in ONE query...`);
            
            try {
                // Load ALL remaining patients in ONE query (simpler than batching!)
                var result = await eel.get_patients_paginated(remaining, currentOffset)();
                
                // Migrate fields for new patients
                for (var i = 0; i < result.patients.length; i++) {
                    var p = result.patients[i];
                    if (p.isSurvivorshipClinic === undefined) p.isSurvivorshipClinic = false;
                    if (p.isOTC === undefined) p.isOTC = false;
                    if (p.isPriorityList === undefined) p.isPriorityList = false;
                }
                
                // Add to patients array
                patients = patients.concat(result.patients);
                
                console.log(`✅ Background load complete: All ${patients.length} patients loaded`);
                
                // Update UI with all patients
                renderPatientList();
                updateStatusCounts(); // Recalculate from full patient list (optional - counts already correct!)
                
                endTiming('background_patient_load');
                
            } catch (error) {
                console.error('Error loading remaining patients:', error);
                endTiming('background_patient_load');
            }
        }

        // File System Operations (HTA-specific)
        // Load database from backend via Eel
        async function loadDatabase() {
            startTiming('loadDatabase_full');
            try {
                // PHASE 1: Load KPIs FIRST (so counts are always correct!)
                startTiming('eel.get_status_counts');
                var kpiCounts = await eel.get_status_counts()();
                endTiming('eel.get_status_counts');
                
                // PHASE 1: Load first 50 patients (FAST - show UI immediately!)
                startTiming('eel.get_patients_paginated_initial');
                var initialResult = await eel.get_patients_paginated(50, 0)();
                endTiming('eel.get_patients_paginated_initial');
                
                patients = initialResult.patients;
                var totalPatients = initialResult.total;
                var hasMore = initialResult.has_more;
                
                if (DEBUG_TIMING) {
                    logTiming(`Loaded KPIs + first ${patients.length} of ${totalPatients} patients (showing UI now!)`);
                }
                
                // Initialize empty structures if needed
                if (!patients) patients = [];
                
                // Initialize clinic days - will be loaded on demand
                clinicDays = {};
                
                // Load current day's clinic configuration
                startTiming('loadCurrentDayClinicData');
                await loadCurrentDayClinicData();
                endTiming('loadCurrentDayClinicData');
                
                // Migrate old patients to add new fields if they don't exist
                startTiming('migrate_patient_fields');
                for (var i = 0; i < patients.length; i++) {
                    if (patients[i].isSurvivorshipClinic === undefined) {
                        patients[i].isSurvivorshipClinic = false;
                    }
                    if (patients[i].isOTC === undefined) {
                        patients[i].isOTC = false;
                    }
                    if (patients[i].isPriorityList === undefined) {
                        patients[i].isPriorityList = false;
                    }
                }
                endTiming('migrate_patient_fields');
                
                // ✨ SHOW UI NOW! (before loading rest of patients)
                if (document.getElementById('mainApp').style.display !== 'block') {
                    document.getElementById('mainApp').style.display = 'block';
                }
                
                // RENDER UI WITH FIRST 50 PATIENTS + CORRECT KPIs
                startTiming('renderPatientList_from_loadDB');
                await renderPatientList();
                endTiming('renderPatientList_from_loadDB');
                
                startTiming('renderAppointments_from_loadDB');
                await renderAppointments();
                endTiming('renderAppointments_from_loadDB');
                
                // Use pre-loaded KPI counts (already fetched from SQL!)
                startTiming('updateStatusCounts_from_loadDB');
                updateStatusCounts(kpiCounts);
                endTiming('updateStatusCounts_from_loadDB');
                
                startTiming('updateClinicTypeButtons');
                updateClinicTypeButtons();
                endTiming('updateClinicTypeButtons');
                
                // Update date display
                updateDateDisplay();
                
                // Initialize main date picker for appointments (uses SQL backend)
                await initializeMainDatePicker();
                
                if (DEBUG_TIMING) {
                    logTiming('Initial UI loaded with CORRECT KPIs - loading remaining patients in background...');
                }
                endTiming('loadDatabase_full');
                
                // PHASE 2: Load remaining patients in background (doesn't block UI!)
                if (hasMore) {
                    loadRemainingPatientsInBackground(50, totalPatients);
                }
                
            } catch (error) {
                console.error("Error loading database:", error);
                showErrorModal("Error loading database: " + error);
                patients = [];
                endTiming('loadDatabase_full');
            }
        }
        
        // Load current day's clinic configuration
        async function loadCurrentDayClinicData() {
            var dateStr = currentViewDate.getFullYear() + '-' +
                ('0' + (currentViewDate.getMonth() + 1)).slice(-2) + '-' +
                ('0' + currentViewDate.getDate()).slice(-2);
            
            try {
                var config = await eel.get_clinic_day(dateStr)();
                if (config) {
                    clinicDays[dateStr] = config;
                } else {
                    // Day has no configuration - remove it from cache
                    delete clinicDays[dateStr];
                }
            } catch (error) {
                console.error('Error loading current day clinic data:', error);
            }
        }
        
        // Load clinic days for a specific month
        async function loadMonthClinicDays(year, month) {
            try {
                var monthData = await eel.get_month_clinic_days(year, month)();
                // Merge into clinicDays
                for (var date in monthData) {
                    clinicDays[date] = monthData[date];
                }
                console.log(`Loaded ${Object.keys(monthData).length} clinic days for ${year}-${month}`);
                return monthData;
            } catch (error) {
                console.error('Error loading month clinic days:', error);
                return {};
            }
        }


        function saveDatabase(silent) {
            // Database saves handled by backend - individual operations call backend directly
            // This function now just marks as saved
            if (!isLoggedIn) return;
            
            if (isReadOnly) {
                if (!silent) {
                    showErrorModal('Cannot save - database is in read-only mode.');
                }
                return;
            }
            
            updateLastSavedTime(silent);
            
            if (!silent) {
                // Show brief confirmation
                console.log('Changes saved to backend');
            }
        }

        function saveClinicDays() {
            // Clinic days saved via backend when toggleClinicType is called
            // No action needed here
        }


        async function backupDatabase() {
            // Close dropdown immediately so user can see the modal
            toggleUserSettingsDropdown();
            
            try {
                // Show loading indicator
                showError('Creating backup...');
                
                var result = await eel.create_backup()();
                
                if (result && result.success) {
                    closeModal('errorModal');
                    showInfo(`Backup created successfully!\n\nFile: ${result.filename}\nSize: ${result.size_mb} MB\n\nLocation: DB/backups/`, 'Backup Complete');
                } else {
                    closeModal('errorModal');
                    showError(`Backup failed: ${result ? result.error : 'Unknown error'}`);
                }
            } catch (error) {
                console.error('Backup error:', error);
                closeModal('errorModal');
                showError('Backup failed: ' + error);
            }
        }
        
        async function checkAndCreateDailyBackup() {
            try {
                var today = new Date().toISOString().split('T')[0].replace(/-/g, ''); // YYYYMMDD
                var lastBackupDate = await eel.get_last_backup_date()();
                
                if (!lastBackupDate || lastBackupDate < today) {
                    console.log('Creating daily auto-backup...');
                    var result = await eel.create_backup()();
                    if (result.success) {
                        console.log(`✓ Daily backup created: ${result.filename} (${result.size_mb} MB)`);
                    }
                }
            } catch (error) {
                console.error('Error checking daily backup:', error);
            }
        }

        // Unsaved changes tracking
        // Auto-save timer removed - all actions save directly to database via backend API


        // Date navigation
        async function changeDate(days) {
            currentViewDate.setDate(currentViewDate.getDate() + days);
            updateDateDisplay();
            await renderAppointments();
            // Reload clinic data from DB
            await loadCurrentDayClinicData();
            updateClinicTypeButtons();
            
            // Sync the flatpickr selected date
            var dateElement = document.getElementById('currentDate');
            if (dateElement && dateElement._flatpickr) {
                dateElement._flatpickr.setDate(currentViewDate, false); // false = don't trigger onChange
            }
        }

        function updateDateDisplay() {
            var options = { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric', timeZone: 'America/Toronto' };
            document.getElementById('currentDate').textContent = currentViewDate.toLocaleDateString('en-US', options);
        }

        // Initialize main date picker for appointments date navigation
        async function initializeMainDatePicker() {
            var dateElement = document.getElementById('currentDate');
            if (dateElement && typeof flatpickr !== 'undefined') {
                try {
                    // Get appointment dates from SQL backend (3-4x faster!)
                    var appointmentDates = await getAllAppointmentDates();
                    flatpickr(dateElement, {
                        dateFormat: 'Y-m-d',
                        defaultDate: currentViewDate,
                        onOpen: async function(selectedDates, dateStr, instance) {
                            // Load current month + adjacent months (for days shown from prev/next month)
                            var currentDate = instance.currentYear && instance.currentMonth !== undefined 
                                ? new Date(instance.currentYear, instance.currentMonth, 1)
                                : currentViewDate;
                            
                            var year = currentDate.getFullYear();
                            var month = currentDate.getMonth() + 1;
                            
                            // Load current month
                            await loadMonthClinicDays(year, month);
                            
                            // Load previous month (for dates shown at start of calendar)
                            var prevMonth = month - 1;
                            var prevYear = year;
                            if (prevMonth === 0) {
                                prevMonth = 12;
                                prevYear--;
                            }
                            await loadMonthClinicDays(prevYear, prevMonth);
                            
                            // Load next month (for dates shown at end of calendar)
                            var nextMonth = month + 1;
                            var nextYear = year;
                            if (nextMonth === 13) {
                                nextMonth = 1;
                                nextYear++;
                            }
                            await loadMonthClinicDays(nextYear, nextMonth);
                            
                            // Force redraw after all data loaded
                            setTimeout(function() {
                                instance.redraw();
                            }, 50);
                        },
                        onMonthChange: async function(selectedDates, dateStr, instance) {
                            // Load new month + adjacent months when month changes
                            var year = instance.currentYear;
                            var month = instance.currentMonth + 1;
                            
                            // Load current month
                            await loadMonthClinicDays(year, month);
                            
                            // Load previous month
                            var prevMonth = month - 1;
                            var prevYear = year;
                            if (prevMonth === 0) {
                                prevMonth = 12;
                                prevYear--;
                            }
                            await loadMonthClinicDays(prevYear, prevMonth);
                            
                            // Load next month
                            var nextMonth = month + 1;
                            var nextYear = year;
                            if (nextMonth === 13) {
                                nextMonth = 1;
                                nextYear++;
                            }
                            await loadMonthClinicDays(nextYear, nextMonth);
                            
                            // Force redraw after all data loaded
                            setTimeout(function() {
                                instance.redraw();
                            }, 50);
                        },
                        onChange: async function(selectedDates) {
                            if (selectedDates.length > 0) {
                                currentViewDate = selectedDates[0];
                                updateDateDisplay();
                                await renderAppointments();
                                // Reload current day's clinic data from DB (in case another user changed it)
                                await loadCurrentDayClinicData();
                                updateClinicTypeButtons();
                            }
                        },
                        onDayCreate: function(dObj, dStr, fp, dayElem) {
                            var dateStr = dayElem.dateObj.getFullYear() + '-' +
                                ('0' + (dayElem.dateObj.getMonth() + 1)).slice(-2) + '-' +
                                ('0' + dayElem.dateObj.getDate()).slice(-2);
                            
                            // Add clinic day type classes
                            var dayData = clinicDays[dateStr];
                            var hasClinicType = dayData && (dayData.md2 || dayData.ivf || dayData.vaughan || dayData.downtown || dayData.survivorship);
                            var hasAppointments = appointmentDates[dateStr];
                            
                            // Add has-appointment class only if no clinic type is defined
                            if (hasAppointments && !hasClinicType) {
                                dayElem.classList.add('has-appointment');
                            }
                            
                            // Add clinic day type classes
                            if (dayData) {
                                if (dayData.md2) dayElem.classList.add('clinic-md2');
                                if (dayData.ivf) dayElem.classList.add('clinic-ivf');
                                if (dayData.vaughan) dayElem.classList.add('clinic-vaughan');
                                if (dayData.downtown) dayElem.classList.add('clinic-downtown');
                                if (dayData.survivorship) dayElem.classList.add('clinic-survivorship');
                            }
                            
                            // Check for open slots (future days with availability)
                            if (hasOpenSlots(dateStr, hasAppointments, dayData)) {
                                dayElem.classList.add('open-slots');
                            }
                        }
                    });
                } catch(e) {
                    // Silently fail
                }
            }
        }

		// Initialize date pickers for modals (edit and schedule appointment)
        function initializeModalDatePickers() {
            var appointmentDates = getAllAppointmentDates();
            
            // Edit appointment date picker
            var editDateElement = document.getElementById('editApptDate');
            if (editDateElement && typeof flatpickr !== 'undefined') {
                try {
                    flatpickr(editDateElement, {
                        dateFormat: 'Y-m-d',
                        onOpen: async function(selectedDates, dateStr, instance) {
                            // Load clinic days for current month + adjacent
                            var currentDate = instance.currentYear && instance.currentMonth !== undefined 
                                ? new Date(instance.currentYear, instance.currentMonth, 1)
                                : new Date();
                            
                            var year = currentDate.getFullYear();
                            var month = currentDate.getMonth() + 1;
                            
                            await loadMonthClinicDays(year, month);
                            await loadMonthClinicDays(year, month - 1 || 12, month === 1 ? year - 1 : year);
                            await loadMonthClinicDays(year, month === 12 ? 1 : month + 1, month === 12 ? year + 1 : year);
                            
                            setTimeout(function() { instance.redraw(); }, 50);
                        },
                        onMonthChange: async function(selectedDates, dateStr, instance) {
                            var year = instance.currentYear;
                            var month = instance.currentMonth + 1;
                            
                            await loadMonthClinicDays(year, month);
                            await loadMonthClinicDays(year, month - 1 || 12, month === 1 ? year - 1 : year);
                            await loadMonthClinicDays(year, month === 12 ? 1 : month + 1, month === 12 ? year + 1 : year);
                            
                            setTimeout(function() { instance.redraw(); }, 50);
                        },
						onChange: function(selectedDates, dateStr, instance) {
							updateDayViewPanel(dateStr);
						},
                        onDayCreate: function(dObj, dStr, fp, dayElem) {
                            var dateStr = dayElem.dateObj.getFullYear() + '-' +
                                ('0' + (dayElem.dateObj.getMonth() + 1)).slice(-2) + '-' +
                                ('0' + dayElem.dateObj.getDate()).slice(-2);
                            
                            // Add clinic day type classes
                            var dayData = clinicDays[dateStr];
                            var hasClinicType = dayData && (dayData.md2 || dayData.ivf || dayData.vaughan || dayData.downtown || dayData.survivorship);
                            var hasAppointments = appointmentDates[dateStr];
                            
                            // Add has-appointment class only if no clinic type is defined
                            if (hasAppointments && !hasClinicType) {
                                dayElem.classList.add('has-appointment');
                            }
                            
                            // Add clinic day type classes
                            if (dayData) {
                                if (dayData.md2) dayElem.classList.add('clinic-md2');
                                if (dayData.ivf) dayElem.classList.add('clinic-ivf');
                                if (dayData.vaughan) dayElem.classList.add('clinic-vaughan');
                                if (dayData.downtown) dayElem.classList.add('clinic-downtown');
                                if (dayData.survivorship) dayElem.classList.add('clinic-survivorship');
                            }
                            
                            // Check for open slots (future days with availability)
                            if (hasOpenSlots(dateStr, hasAppointments, dayData)) {
                                dayElem.classList.add('open-slots');
                            }
                        }
                    });
                } catch(e) {
                    // Silently fail
                }
            }
            
            // Schedule appointment date picker (transitionDate)
            var transitionDateElement = document.getElementById('transitionDate');
            if (transitionDateElement && typeof flatpickr !== 'undefined') {
                try {
                    flatpickr(transitionDateElement, {
                        dateFormat: 'Y-m-d',
                        onOpen: async function(selectedDates, dateStr, instance) {
                            // Load clinic days for current month + adjacent
                            var currentDate = instance.currentYear && instance.currentMonth !== undefined 
                                ? new Date(instance.currentYear, instance.currentMonth, 1)
                                : new Date();
                            
                            var year = currentDate.getFullYear();
                            var month = currentDate.getMonth() + 1;
                            
                            await loadMonthClinicDays(year, month);
                            await loadMonthClinicDays(year, month - 1 || 12, month === 1 ? year - 1 : year);
                            await loadMonthClinicDays(year, month === 12 ? 1 : month + 1, month === 12 ? year + 1 : year);
                            
                            setTimeout(function() { instance.redraw(); }, 50);
                        },
                        onMonthChange: async function(selectedDates, dateStr, instance) {
                            var year = instance.currentYear;
                            var month = instance.currentMonth + 1;
                            
                            await loadMonthClinicDays(year, month);
                            await loadMonthClinicDays(year, month - 1 || 12, month === 1 ? year - 1 : year);
                            await loadMonthClinicDays(year, month === 12 ? 1 : month + 1, month === 12 ? year + 1 : year);
                            
                            setTimeout(function() { instance.redraw(); }, 50);
                        },
						onChange: function(selectedDates, dateStr, instance) {
							updateDayViewPanel(dateStr);
    					},
                        onDayCreate: function(dObj, dStr, fp, dayElem) {
                            var dateStr = dayElem.dateObj.getFullYear() + '-' +
                                ('0' + (dayElem.dateObj.getMonth() + 1)).slice(-2) + '-' +
                                ('0' + dayElem.dateObj.getDate()).slice(-2);
                            
                            // Add clinic day type classes
                            var dayData = clinicDays[dateStr];
                            var hasClinicType = dayData && (dayData.md2 || dayData.ivf || dayData.vaughan || dayData.downtown || dayData.survivorship);
                            var hasAppointments = appointmentDates[dateStr];
                            
                            // Add has-appointment class only if no clinic type is defined
                            if (hasAppointments && !hasClinicType) {
                                dayElem.classList.add('has-appointment');
                            }
                            
                            // Add clinic day type classes
                            if (dayData) {
                                if (dayData.md2) dayElem.classList.add('clinic-md2');
                                if (dayData.ivf) dayElem.classList.add('clinic-ivf');
                                if (dayData.vaughan) dayElem.classList.add('clinic-vaughan');
                                if (dayData.downtown) dayElem.classList.add('clinic-downtown');
                                if (dayData.survivorship) dayElem.classList.add('clinic-survivorship');
                            }
                            
                            // Check for open slots (future days with availability)
                            if (hasOpenSlots(dateStr, hasAppointments, dayData)) {
                                dayElem.classList.add('open-slots');
                            }
                        }
                    });
                } catch(e) {
                    // Silently fail
                }
            }
        }

        // Refresh the main date picker to update clinic day styling
        async function refreshMainDatePicker() {
            var dateElement = document.getElementById('currentDate');
            if (dateElement && dateElement._flatpickr) {
                dateElement._flatpickr.destroy();
            }
            await initializeMainDatePicker();
        }

        // Toggle clinic type for current date
        function toggleClinicType(type) {
            // Block in read-only mode
            if (isReadOnly) {
                showErrorModal('Cannot modify clinic days - database is in read-only mode.\nAnother user (' + lockOwner + ') is currently editing.');
                return;
            }
            
            var dateStr = currentViewDate.getFullYear() + '-' +
							('0' + (currentViewDate.getMonth() + 1)).slice(-2) + '-' +
							('0' + currentViewDate.getDate()).slice(-2); //currentViewDate.toISOString().split('T')[0];
            
            // Initialize the date entry if it doesn't exist
            if (!clinicDays[dateStr]) {
                clinicDays[dateStr] = {};
            }
            
            var currentValue = clinicDays[dateStr][type] || false;
            
            // Handle mutual exclusivity
            if (type === 'ivf' && !currentValue) {
                // IVF clears everything else
                clinicDays[dateStr] = { ivf: true };
            } else if (type === 'vaughan' && !currentValue) {
                // Vaughan clears Downtown
                clinicDays[dateStr].vaughan = true;
                clinicDays[dateStr].downtown = false;
                clinicDays[dateStr].ivf = false;
            } else if (type === 'downtown' && !currentValue) {
                // Downtown clears Vaughan
                clinicDays[dateStr].downtown = true;
                clinicDays[dateStr].vaughan = false;
                clinicDays[dateStr].ivf = false;
            } else if ((type === 'survivorship' || type === 'md2') && !currentValue) {
                // These can be added but clear IVF
                clinicDays[dateStr][type] = true;
                clinicDays[dateStr].ivf = false;
            } else {
                // Toggle off
                clinicDays[dateStr][type] = !currentValue;
            }
            
            // Clean up empty entries
            var hasAnyValue = false;
            for (var key in clinicDays[dateStr]) {
                if (clinicDays[dateStr][key]) {
                    hasAnyValue = true;
                    break;
                }
            }
            if (!hasAnyValue) {
                delete clinicDays[dateStr];
            }
            
            // Save to backend immediately
            saveClinicDayToBackend(dateStr, clinicDays[dateStr] || {});
            
            // No need for markAsChanged() - already saved to backend!
            updateClinicTypeButtons();
            refreshMainDatePicker();
        }
        
        // Save clinic day configuration to backend
        async function saveClinicDayToBackend(dateStr, config) {
            try {
                await eel.update_clinic_day(dateStr, config)();
                console.log(`Saved clinic day ${dateStr}:`, config);
            } catch (error) {
                console.error('Error saving clinic day:', error);
            }
        }
        
        // Load clinic day configuration from backend
        async function loadClinicDayFromBackend(date) {
            var dateStr = date.getFullYear() + '-' +
                ('0' + (date.getMonth() + 1)).slice(-2) + '-' +
                ('0' + date.getDate()).slice(-2);
            
            try {
                var config = await eel.get_clinic_day(dateStr)();
                if (config) {
                    clinicDays[dateStr] = config;
                }
                updateClinicTypeButtons();
            } catch (error) {
                console.error('Error loading clinic day:', error);
            }
        }

        // Update clinic type button states based on current date
        function updateClinicTypeButtons() {
            var dateStr = currentViewDate.getFullYear() + '-' +
				('0' + (currentViewDate.getMonth() + 1)).slice(-2) + '-' +
				('0' + currentViewDate.getDate()).slice(-2); //currentViewDate.toISOString().split('T')[0];
            var dayData = clinicDays[dateStr] || {};
            
            var types = ['vaughan', 'downtown', 'survivorship', 'ivf', 'md2'];
            for (var i = 0; i < types.length; i++) {
                var type = types[i];
                var btn = document.getElementById('btn' + type.charAt(0).toUpperCase() + type.slice(1));
                if (btn) {
                    if (dayData[type]) {
                        btn.classList.add('active');
                    } else {
                        btn.classList.remove('active');
                    }
                }
            }
        }

		// Get all appointment dates for calendar highlighting (uses SQL backend for speed)
		async function getAllAppointmentDates() {
			try {
				// Use SQL backend (3-4x faster than JS loops!)
				var dateList = await eel.get_all_appointment_dates()();
				
				// Convert array to object for fast lookup
				var dates = {};
				for (var i = 0; i < dateList.length; i++) {
					dates[dateList[i]] = true;
				}
				return dates;
			} catch (error) {
				console.error('Error getting appointment dates from backend:', error);
				
				// Fallback to JS method if backend fails
				var dates = {};
				for (var i = 0; i < patients.length; i++) {
					// Future appointments
					if (patients[i].nextAppointment) {
						dates[patients[i].nextAppointment] = true;
					}
					// Past appointments from history
					if (patients[i].appointmentHistory) {
						for (var j = 0; j < patients[i].appointmentHistory.length; j++) {
							dates[patients[i].appointmentHistory[j].date] = true;
						}
					}
				}
				return dates;
			}
		}

        // Get all appointment times for a specific date (non-cancelled only)
        function getAppointmentTimesForDate(dateStr) {
            var times = [];
            
            for (var i = 0; i < patients.length; i++) {
                // Check future appointments
                if (patients[i].nextAppointment === dateStr && patients[i].appointmentTime) {
                    times.push(patients[i].appointmentTime);
                }
                
//                // Check appointment history
//                if (patients[i].appointmentHistory) {
//                    for (var j = 0; j < patients[i].appointmentHistory.length; j++) {
//                        var appt = patients[i].appointmentHistory[j];
//                        if (appt.date === dateStr && appt.time) {
//                            // Skip cancelled appointments
//                            var isCancelled = appt.summary && appt.summary.toLowerCase().indexOf('cancelled') !== -1;
//							var isRescheduled = appt.summary && appt.summary.toLowerCase().indexOf('rescheduled') !== -1;
//                            if (!isCancelled && !isRescheduled) {
//                                times.push(appt.time);
//                            }
//                        }
//                    }
//                }
            }
            
            return times;
        }

        // Check if a day is fully booked
        // Morning: 8:00, 8:30, 9:00, 9:30, 10:00, 10:30, 11:00, 11:30 (8 slots)
        // Afternoon: 13:00, 13:30, 14:00, 14:30, 15:00, 15:30 (6 slots)
        function isDayFullyBooked(dateStr) {
            var times = getAppointmentTimesForDate(dateStr);

			// Check if this is an MD2 day
			var dayData = clinicDays[dateStr] || {};
			var isMD2Day = dayData.md2 || false;
            
            // Required morning slots
            var morningSlots = isMD2Day 
				? ['08:00', '08:30', '09:00', '09:30', '10:00', '10:30']
				: ['08:00', '08:30', '09:00', '09:30', '10:00', '10:30', '11:00', '11:30'];
            // Required afternoon slots
            var afternoonSlots = isMD2Day
				? ['13:30', '14:00', '14:30', '15:00', '15:30']
				: ['13:00', '13:30', '14:00', '14:30', '15:00', '15:30'];
            
            // Check morning - all slots must be filled
            var morningFull = true;
            for (var i = 0; i < morningSlots.length; i++) {
                if (times.indexOf(morningSlots[i]) === -1) {
                    morningFull = false;
                    break;
                }
            }
            
            // Check afternoon - all slots must be filled
            var afternoonFull = true;
            for (var j = 0; j < afternoonSlots.length; j++) {
                if (times.indexOf(afternoonSlots[j]) === -1) {
                    afternoonFull = false;
                    break;
                }
            }
            
            return morningFull && afternoonFull;
        }

        // Check if a day has open slots (future day, has appointments or clinic type, not fully booked)
        function hasOpenSlots(dateStr, hasAppointments, dayData) {
            // Get today's date
            var today = getTodayLocalDate();
            
            // Must be a future day
            if (dateStr <= today) {
                return false;
            }
            
            // IVF days don't have open slots concept (ring only)
            if (dayData && dayData.ivf) {
                return false;
            }
            
            // Must have appointments or a clinic type set (excluding IVF)
            var hasClinicType = dayData && (dayData.vaughan || dayData.downtown || dayData.survivorship || dayData.md2);
            if (!hasAppointments && !hasClinicType) {
                return false;
            }
            
            // Check if fully booked
            return !isDayFullyBooked(dateStr);
        }

        // Get free time slots for a specific date
        function getFreeSlotsForDate(dateStr) {
            var bookedTimes = getAppointmentTimesForDate(dateStr);
			
			// Check if this is an MD2 day
			var dayData = clinicDays[dateStr] || {};
			var isMD2Day = dayData.md2 || false;
			
            var allSlots = isMD2Day
				? [
					'08:00', '08:30', '09:00', '09:30', '10:00', '10:30',
					'13:30', '14:00', '14:30', '15:00', '15:30'
					]
				: [
					'08:00', '08:30', '09:00', '09:30', '10:00', '10:30', '11:00', '11:30', 
					'13:00', '13:30', '14:00', '14:30', '15:00', '15:30'
					];
            
            var freeSlots = [];
            for (var i = 0; i < allSlots.length; i++) {
                if (bookedTimes.indexOf(allSlots[i]) === -1) {
                    freeSlots.push(allSlots[i]);
                }
            }
            return freeSlots;
        }

        // Format date for display (e.g., "FRIDAY DECEMBER 5")
        function formatDateForDisplay(dateStr) {
            var parts = dateStr.split('-');
            var date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
            
            var days = ['SUNDAY', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY'];
            var months = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 
                          'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER'];
            
            var dayName = days[date.getDay()];
            var monthName = months[date.getMonth()];
            var dayNum = date.getDate();
            
            return dayName + ' ' + monthName + ' ' + dayNum;
        }

        // Get all free slots grouped by clinic type
        // Current filter for free slots modal
        var freeSlotsFilter = 'asap'; // 'asap', '6-8wks', '3months+'

        function getAllFreeSlots(minDays, maxDays) {
            var today = getTodayLocalDate();
            var appointmentDates = getAllAppointmentDates();
            
            // Track dates used by survivorship to exclude from vaughan/downtown
            var survivorshipDates = {};
            
            var result = {
                vaughan: [],
                downtown: [],
                survivorship: []
            };
            
            // First pass: collect survivorship dates
            var currentDate = new Date();
            var startDate = new Date();
            startDate.setDate(startDate.getDate() + minDays);
            
            for (var i = minDays; i <= maxDays; i++) {
                var checkDate = new Date();
                checkDate.setDate(checkDate.getDate() + i);
                var dateStr = checkDate.getFullYear() + '-' +
                    ('0' + (checkDate.getMonth() + 1)).slice(-2) + '-' +
                    ('0' + checkDate.getDate()).slice(-2);
                
                var dayData = clinicDays[dateStr];
                if (!dayData) continue;
                if (dayData.ivf) continue;
                if (!dayData.survivorship) continue;
                
                var hasAppointments = appointmentDates[dateStr];
                if (!hasOpenSlots(dateStr, hasAppointments, dayData)) continue;
                
                var freeSlots = getFreeSlotsForDate(dateStr);
                if (freeSlots.length === 0) continue;
                
                // Get max 2 morning and max 2 afternoon slots
                var limitedSlots = [];
                var morningCount = 0;
                var afternoonCount = 0;
                for (var j = 0; j < freeSlots.length; j++) {
                    var hour = parseInt(freeSlots[j].split(':')[0]);
                    if (hour < 12 && morningCount < 3) {
                        limitedSlots.push(freeSlots[j]);
                        morningCount++;
                    } else if (hour >= 13 && afternoonCount < 3) {
                        limitedSlots.push(freeSlots[j]);
                        afternoonCount++;
                    }
                }
                
                survivorshipDates[dateStr] = true;
                result.survivorship.push({
                    date: dateStr,
                    displayDate: formatDateForDisplay(dateStr),
                    slots: limitedSlots
                });
            }
            
            // Second pass: collect vaughan and downtown (excluding survivorship dates)
            for (var i = minDays; i <= maxDays; i++) {
                var checkDate = new Date();
                checkDate.setDate(checkDate.getDate() + i);
                var dateStr = checkDate.getFullYear() + '-' +
                    ('0' + (checkDate.getMonth() + 1)).slice(-2) + '-' +
                    ('0' + checkDate.getDate()).slice(-2);
                
                // Skip if this date is used by survivorship
                if (survivorshipDates[dateStr]) continue;
                
                var dayData = clinicDays[dateStr];
                if (!dayData) continue;
                if (dayData.ivf) continue;
                
                var hasAppointments = appointmentDates[dateStr];
                if (!hasOpenSlots(dateStr, hasAppointments, dayData)) continue;
                
                var freeSlots = getFreeSlotsForDate(dateStr);
                if (freeSlots.length === 0) continue;
                
                // Get max 2 morning and max 2 afternoon slots
                var limitedSlots = [];
                var morningCount = 0;
                var afternoonCount = 0;
                for (var j = 0; j < freeSlots.length; j++) {
                    var hour = parseInt(freeSlots[j].split(':')[0]);
                    if (hour < 12 && morningCount < 3) {
                        limitedSlots.push(freeSlots[j]);
                        morningCount++;
                    } else if (hour >= 13 && afternoonCount < 3) {
                        limitedSlots.push(freeSlots[j]);
                        afternoonCount++;
                    }
                }
                
                var slotData = {
                    date: dateStr,
                    displayDate: formatDateForDisplay(dateStr),
                    slots: limitedSlots
                };
                
                if (dayData.vaughan) {
                    result.vaughan.push(slotData);
                } else if (dayData.downtown) {
                    result.downtown.push(slotData);
                }
            }
            
            return result;
        }

        // Navigate to a specific date
        function goToDate(dateStr) {
            var parts = dateStr.split('-');
            currentViewDate = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
            updateDateDisplay();
            renderAppointments();
            updateClinicTypeButtons();
            refreshMainDatePicker();
            closeModal('freeSlotsModal');
        }

        // Set free slots filter and refresh
        function setFreeSlotsFilter(filter) {
            freeSlotsFilter = filter;
            renderFreeSlotsContent();
            
            // Update button states
            var buttons = document.querySelectorAll('.free-slots-filter-btn');
            for (var i = 0; i < buttons.length; i++) {
                buttons[i].classList.remove('active');
            }
            document.getElementById('filterBtn' + filter.replace('-', '').replace('+', '')).classList.add('active');
        }

        // Render free slots content based on current filter
        function renderFreeSlotsContent() {
            var minDays, maxDays;
            
            if (freeSlotsFilter === 'asap') {
                minDays = 1;
                maxDays = 180;
            } else if (freeSlotsFilter === '6-8wks') {
                minDays = 42;
                maxDays = 180;
            } else { // 3months+
                minDays = 90;
                maxDays = 360;
            }
            
            var freeSlots = getAllFreeSlots(minDays, maxDays);
            var content = document.getElementById('freeSlotsContent');
            var html = '';
            var maxDatesPerCategory = 7;
            
            // Helper function to render a category
            function renderCategory(categoryName, color, slots) {
                var categoryHtml = '<div style="margin-bottom: 10px;">';
                categoryHtml += '<h3 style="color: ' + color + '; margin-bottom: 4px; font-size: 14px; border-bottom: 2px solid ' + color + '; padding-bottom: 2px;">' + categoryName + '</h3>';
                
                if (slots.length === 0) {
                    categoryHtml += '<div style="color: #999; font-size: 12px; padding: 4px 0;">No free slots available in this time range.</div>';
                } else {
                    var dateCount = Math.min(slots.length, maxDatesPerCategory);
                    
                    for (var i = 0; i < dateCount; i++) {
                        var day = slots[i];
                        var slotsDisplay = day.slots.join(', ');
                        
                        categoryHtml += '<div style="display: flex; justify-content: space-between; align-items: center; padding: 3px 8px; background: ' + (i % 2 === 0 ? '#fff' : '#f9f9f9') + '; border-radius: 3px; margin-bottom: 2px;">';
                        categoryHtml += '<span style="font-size: 12px;">' + (i + 1) + '. ' + day.displayDate + ' @ ' + slotsDisplay + '</span>';
                        categoryHtml += '<button class="btn btn-small btn-primary" onclick="goToDate(\'' + day.date + '\')" style="padding: 2px 6px; font-size: 10px;">Go</button>';
                        categoryHtml += '</div>';
                    }
                }
                
                categoryHtml += '</div>';
                return categoryHtml;
            }
            
            // Render all three categories
            html += renderCategory('Vaughan', '#f1c40f', freeSlots.vaughan);
            html += renderCategory('Downtown', '#e67e22', freeSlots.downtown);
            html += renderCategory('Survivorship', '#9b59b6', freeSlots.survivorship);
            
            content.innerHTML = html;
        }

        // Open Free Slots modal
        function openFreeSlotsModal() {
            freeSlotsFilter = 'asap'; // Reset to default
            renderFreeSlotsContent();
            
            // Reset button states
            var buttons = document.querySelectorAll('.free-slots-filter-btn');
            for (var i = 0; i < buttons.length; i++) {
                buttons[i].classList.remove('active');
            }
            document.getElementById('filterBtnasap').classList.add('active');
            
            document.getElementById('freeSlotsModal').classList.add('active');
        }

// ============================================================================
        // REMINDERS FUNCTIONS
        // ============================================================================
        
        // Format relative time (e.g., "5 days ago", "2 weeks, 1 day ago")
        function formatRelativeTime(dateStr) {
            if (!dateStr) return 'Unknown';
            
            var setDate = new Date(dateStr);
            var now = new Date();
            var diffMs = now - setDate;
            var diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
            
            if (diffDays === 0) return 'Today';
            if (diffDays === 1) return '1 day ago';
            
            var weeks = Math.floor(diffDays / 7);
            var days = diffDays % 7;
            
            if (weeks === 0) {
                return diffDays + ' days ago';
            } else if (days === 0) {
                return weeks + (weeks === 1 ? ' week ago' : ' weeks ago');
            } else {
                return weeks + (weeks === 1 ? ' week, ' : ' weeks, ') + days + (days === 1 ? ' day ago' : ' days ago');
            }
        }
        
        // Format date for display (e.g., "Dec. 20, 2025")
        function formatSetDate(dateStr) {
            if (!dateStr) return 'Unknown';
            
            var date = new Date(dateStr);
            var months = ['Jan.', 'Feb.', 'Mar.', 'Apr.', 'May', 'Jun.', 'Jul.', 'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.'];
            return months[date.getMonth()] + ' ' + date.getDate() + ', ' + date.getFullYear();
        }
        
        // Get appointment set timestamp from state history
        function getAppointmentSetTimestamp(patient) {
            if (!patient.stateHistory || patient.stateHistory.length === 0) return null;
            
            // Find the most recent WAITING_FIRST_APPT or WAITING_NEXT_APPT state
            for (var i = patient.stateHistory.length - 1; i >= 0; i--) {
                var state = patient.stateHistory[i].state;
                if (state === 'WAITING_FIRST_APPT' || state === 'WAITING_NEXT_APPT') {
                    return patient.stateHistory[i].timestamp;
                }
            }
            return null;
        }
        
        // Open reminders modal
        function openRemindersModal() {
            var dateStr = currentViewDate.getFullYear() + '-' +
                ('0' + (currentViewDate.getMonth() + 1)).slice(-2) + '-' +
                ('0' + currentViewDate.getDate()).slice(-2);
            
            var today = getTodayLocalDate();
            
            // Only allow for today or future dates
            if (dateStr < today) {
                showErrorModal('Reminders can only be sent for today or future appointments.');
                return;
            }
            
            // Get appointments for this date (non-cancelled only)
            var appointmentsForDate = [];
            for (var i = 0; i < patients.length; i++) {
                var p = patients[i];
                if (p.nextAppointment === dateStr) {
                    appointmentsForDate.push(p);
                }
            }
            
            if (appointmentsForDate.length === 0) {
                showErrorModal('No appointments scheduled for this date.');
                return;
            }
            
            // Sort by appointment time
            appointmentsForDate.sort(function(a, b) {
                var timeA = a.appointmentTime || '00:00';
                var timeB = b.appointmentTime || '00:00';
                return timeA.localeCompare(timeB);
            });
            
            // Build content
            var html = '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
            html += '<thead><tr style="background: #f5f5f5;">';
            html += '<th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd; width: 30px;"></th>';
            html += '<th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Patient</th>';
            html += '<th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Time</th>';
            html += '<th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">Set Date</th>';
            html += '</tr></thead><tbody>';
            
			var seenTimes = {};
            for (var j = 0; j < appointmentsForDate.length; j++) {
                var patient = appointmentsForDate[j];
                var setTimestamp = getAppointmentSetTimestamp(patient);
                var setDateDisplay = setTimestamp ? formatSetDate(setTimestamp) + ' (' + formatRelativeTime(setTimestamp) + ')' : 'Unknown';
                var hasPartner = patient.partnerName && patient.partnerName.trim() !== '';
                
                // Check if this time slot was already seen (same appointment for couple)
                var apptTime = patient.appointmentTime || 'TBD';
                var isDuplicateTime = seenTimes[apptTime];
                seenTimes[apptTime] = true;
                
                // Check if appointment was set less than 1 week ago
                var isRecentlySet = false;
                if (setTimestamp) {
                    var setDate = new Date(setTimestamp);
                    var now = new Date();
                    var daysSinceSet = (now - setDate) / (1000 * 60 * 60 * 24);
                    isRecentlySet = daysSinceSet < 7;
                }
                
                // Uncheck if duplicate time OR recently set
                var isChecked = !isDuplicateTime && !isRecentlySet;
				
                html += '<tr style="border-bottom: 1px solid #eee;">';
                html += '<td style="padding: 8px;"><input type="checkbox" class="reminder-checkbox" data-patient-id="' + patient.patientID + '"' + (isChecked ? ' checked' : '') + '></td>';
                html += '<td style="padding: 8px;">';
				html += '<strong>' + formatNameWithAlias(patient.patientName, patient.patientAlias, patient.patientFirstName, patient.patientMiddleName, patient.patientLastName) + '</strong>';
                if (hasPartner) {
                    html += '<br><span style="color: #666; font-size: 11px;">& ' + formatNameWithAlias(patient.partnerName, patient.partnerAlias, patient.partnerFirstName, patient.partnerMiddleName, patient.partnerLastName) + '</span>';                html += '</td>';
				};
                html += '<td style="padding: 8px;">' + (patient.appointmentTime || 'TBD') + '</td>';
                html += '<td style="padding: 8px; font-size: 11px;">' + setDateDisplay + '</td>';
                html += '</tr>';
            }
            
            html += '</tbody></table>';
            
            document.getElementById('remindersContent').innerHTML = html;
            document.getElementById('reminderSelectAll').checked = true;
            document.getElementById('remindersModal').classList.add('active');
        }
        
        // Toggle all reminder checkboxes
        function toggleAllReminders() {
            var selectAll = document.getElementById('reminderSelectAll').checked;
            var checkboxes = document.querySelectorAll('.reminder-checkbox');
            for (var i = 0; i < checkboxes.length; i++) {
                checkboxes[i].checked = selectAll;
            }
        }
        
		async function sendReminders() {
            var checkboxes = document.querySelectorAll('.reminder-checkbox:checked');
            
            if (checkboxes.length === 0) {
                showErrorModal('Please select at least one patient.');
                return;
            }
            
            // Load email templates if not loaded
            if (!emailTemplates) {
                var loaded = await loadEmailTemplates();
                if (!loaded) {
                    showErrorModal('Failed to load email templates');
                    return;
                }
            }
            
            var template = emailTemplates.templates.reminder;
            if (!template) {
                showErrorModal('Reminder email template not found.');
                return;
            }
            
            // Build array of emails
            var emailsArray = [];
            
            // Process each selected patient
            for (var i = 0; i < checkboxes.length; i++) {
                var patientID = checkboxes[i].getAttribute('data-patient-id');
                var patient = null;
                
                for (var j = 0; j < patients.length; j++) {
                    if (patients[j].patientID === patientID) {
                        patient = patients[j];
                        break;
                    }
                }
                
                if (!patient) continue;
                
                // Build email data for this patient
                var emailData = buildReminderEmailData(patient, template);
                if (emailData) {
                    emailsArray.push(emailData);
                }
            }
            
            if (emailsArray.length > 0) {
                await saveEmailsToFile(emailsArray);
            }
            
            closeModal('remindersModal');
        }
        
		// Build reminder email data for a patient (returns object, doesn't open Outlook)
        function buildReminderEmailData(patient, template) {
			var patientFirstName = getFirstName(patient.patientName, patient.patientAlias, patient.patientFirstName);
            var partnerFirstName = getFirstName(patient.partnerName, patient.partnerAlias, patient.partnerFirstName);
            var hasPartner = patient.partnerName && patient.partnerName.trim() !== '';
            
            // Build greeting
            var greeting = hasPartner ? patientFirstName + ' and ' + partnerFirstName : patientFirstName;
            
            // Build recipient list
            var toList = [];
            if (patient.patientEmail || patient.email) {
                toList.push(patient.patientEmail || patient.email);
            }
            if (hasPartner && patient.partnerEmail) {
                toList.push(patient.partnerEmail);
            }
            
            if (toList.length === 0) {
                showErrorModal('No email address for patient: ' + patient.patientName);
                return null;
            }
            
            // Format appointment date
            var appointmentDay = '';
            var formattedDate = '';
            var appointmentDayDate = '';
            
            if (patient.nextAppointment) {
                var dateParts = patient.nextAppointment.split('-');
                var dateObj = new Date(parseInt(dateParts[0]), parseInt(dateParts[1]) - 1, parseInt(dateParts[2]));
                var days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
                var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
                appointmentDay = days[dateObj.getDay()];
                formattedDate = months[dateObj.getMonth()] + ' ' + dateObj.getDate() + ', ' + dateObj.getFullYear();
                appointmentDayDate = appointmentDay + ', ' + formattedDate;
            }
            
			var apptTime = patient.appointmentTime || 'TBD';
            var apptLocation = patient.appointmentLocation || '';
			
            // Build appointment details - use location-aware template if location is set
            var appointmentDetails = '';
            if (apptLocation) {
                appointmentDetails = emailTemplates.commonBlocks.appointmentDetails || '';
                var appointmentType = (apptLocation === 'Virtual') ? 'Virtual (OTN)' : 'In Person';
                var locationInfo = (apptLocation === 'Virtual') ? '' : '<br>Address: ' + (emailTemplates.locations[apptLocation.toLowerCase()] ? emailTemplates.locations[apptLocation.toLowerCase()].address : apptLocation);
                appointmentDetails = appointmentDetails.replace(/\{\{appointmentType\}\}/g, appointmentType);
                appointmentDetails = appointmentDetails.replace(/\{\{locationInfo\}\}/g, locationInfo);
            } else {
                appointmentDetails = emailTemplates.commonBlocks.appointmentDetailsNoLocation || '';
            }
            appointmentDetails = appointmentDetails.replace(/\{\{appointmentDayDate\}\}/g, appointmentDayDate);
            appointmentDetails = appointmentDetails.replace(/\{\{appointmentTime\}\}/g, apptTime);
            
            // Build subject
            var subject = template.subject;
            var partnerFirstNameForSubject = hasPartner ? ' and ' + partnerFirstName : '';
            subject = subject.replace(/\{\{patientFirstName\}\}/g, patientFirstName);
            subject = subject.replace(/\{\{partnerFirstName\}\}/g, partnerFirstNameForSubject);
            
            // Build body
            var body = template.body;
            body = body.replace(/\{\{greeting\}\}/g, greeting);
            body = body.replace(/\{\{patientFirstName\}\}/g, patientFirstName);
            body = body.replace(/\{\{appointmentDetails\}\}/g, appointmentDetails);
            
            // Add OTN block only for virtual appointments
            var otnBlock = (apptLocation === 'Virtual') ? (emailTemplates.commonBlocks.otnBlock || '') : '';
            body = body.replace(/\{\{otnBlock\}\}/g, otnBlock);
            body = body.replace(/\{\{signature\}\}/g, emailTemplates.settings.signature.replace(/\n/g, '<br>'));
            body = body.replace(/\{\{disclaimers\}\}/g, emailTemplates.commonBlocks.disclaimers || '');
            
            return {
                to: toList.join('; '),
                subject: subject,
                body: body,
                account: EMAIL_FROM_ADDRESS
            };
        }
        
        // Save emails array to file for Outlook VBA to process
        async function saveEmailsToFile(emailsArray) {
            // Save as JSON to match HTA format
            var content = JSON.stringify(emailsArray, null, 2);
            
            // Save to DB/pending-emails.json (matches HTA)
            try {
                var result = await eel.save_email_to_file(content, 'pending-emails.json')();
                
                if (result.status === 'success') {
                    showInfo(
                        emailsArray.length + ' email(s) saved to:\n\n' + 
                        result.filepath + '\n\n' +
                        'In Outlook, click the "Create Emails" button to open them.',
                        'Emails Saved'
                    );
                } else {
                    showErrorModal('Error saving emails: ' + result.message);
                }
            } catch (error) {
                console.error('Error saving emails:', error);
                showErrorModal('Error saving emails to file');
            }
        }
		
        // Appointments rendering - SORTED BY TIME, LIMITED TO 5 WITH SCROLLING
        async function renderAppointments() {
            // TRIGGER 1: Smart refresh when opening daily view (skip on initial load)
            if (typeof doSmartRefresh !== 'undefined') {
                await doSmartRefresh('open_daily_view');
            }
            
            var container = document.getElementById('appointmentsList');
            var dateStr = currentViewDate.getFullYear() + '-' +
							('0' + (currentViewDate.getMonth() + 1)).slice(-2) + '-' +
							('0' + currentViewDate.getDate()).slice(-2); //currentViewDate.toISOString().split('T')[0];
            var today = getTodayLocalDate(); // new Date().toISOString().split('T')[0];
            var isPastDate = dateStr < today;
            
            var appointments = [];
            var firstApptCount = 0;
            
            // Get appointments from backend (SQL query!)
            var result;
            try {
                result = await eel.get_appointments_for_date(dateStr)();
            } catch (error) {
                console.error('Error fetching appointments:', error);
                // Fallback to old method if backend fails
                result = {future: [], past: []};
            }
            
            // Map future appointments to format expected by UI
            var futureAppointments = result.future.map(function(appt) {
                var isFirstAppt = appt.isFirstAppt === 1;
                if (isFirstAppt) {
                    firstApptCount++;
                }
                
                // Find full patient object for additional info
                var fullPatient = patients.find(function(p) { return p.patientID === appt.patientID; });
                
                return {
                    patient: fullPatient || appt,  // Use full patient if available
                    date: appt.date,
                    time: appt.time || '00:00',
                    location: appt.location || '',
                    isFuture: true,
                    isFirstAppt: isFirstAppt,
                    summary: null
                };
            });
            
            // Map past appointments to format expected by UI
            var pastAppointments = result.past.map(function(appt) {
                // Find full patient object for additional info
                var fullPatient = patients.find(function(p) { return p.patientID === appt.patientID; });
                
                return {
                    patient: fullPatient || appt,  // Use full patient if available
                    date: appt.date,
                    time: appt.time || '00:00',
                    location: appt.location || '',
                    isFuture: false,
                    isFirstAppt: false,
                    summary: appt.summary
                };
            });
            
            // Combine and sort by time
            appointments = futureAppointments.concat(pastAppointments);
            appointments.sort(function(a, b) { return a.time.localeCompare(b.time); });

            // Count non-cancelled appointments
            var nonCancelledCount = 0;
            for (var i = 0; i < appointments.length; i++) {
                var isCancelled = appointments[i].summary && appointments[i].summary.toLowerCase().indexOf('cancelled') !== -1;
                if (!isCancelled) {
                    nonCancelledCount++;
                }
            }

            // Update header with first appointment count - always show counter
            var headerTitle = 'Appointments (' + firstApptCount + '/' + nonCancelledCount + ' - 1st appt)';
            document.querySelector('.appointments-header .card-title').textContent = headerTitle;

            // Update Reminders button state (only enabled for today/future)
            var btnReminders = document.getElementById('btnReminders');
            if (btnReminders) {
                var today = getTodayLocalDate();
                if (dateStr < today) {
                    btnReminders.disabled = true;
                    btnReminders.style.opacity = '0.5';
                    btnReminders.style.cursor = 'not-allowed';
                } else {
                    btnReminders.disabled = false;
                    btnReminders.style.opacity = '1';
                    btnReminders.style.cursor = 'pointer';
                }
            }

            if (appointments.length === 0) {
                container.innerHTML = '<div class="empty-state">No appointments scheduled for this date</div>';
                return;
            }

            container.innerHTML = appointments.map(function(appt) {
                // Check if appointment is cancelled
                var isCancelled = appt.summary && appt.summary.toLowerCase().indexOf('cancelled') !== -1;
				var isRescheduled = appt.summary && appt.summary.toLowerCase().indexOf('rescheduled') !== -1;
                
                var bgColor = (isCancelled || isRescheduled) ? '#ffebee' : (appt.isFuture ? '#f8f9fa' : '#e8f5e9');
                var borderColor = (isCancelled || isRescheduled) ? '#e74c3c' : (appt.isFuture ? '#3498db' : '#27ae60');
                var statusBadge = appt.isFuture ? '' : '<span style="background: #27ae60; color: white; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-left: 8px;">COMPLETED</span>';
                
				// Add 1ST APPT badge for future first appointments
                var firstApptBadge = (appt.isFuture && appt.isFirstAppt) 
                    ? '<span style="background: #9b59b6; color: white; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-left: 8px;">1ST APPT</span>' 
                    : '';
                
                // Add location badge
                var locationBadge = '';
                if (appt.location) {
                    var locColor = '#b19cd9'; // Lilac for Virtual (default)
                    var locTextColor = 'white';
                    if (appt.location === 'Vaughan') {
                        locColor = '#f1c40f';
                        locTextColor = '#333';
                    } else if (appt.location === 'Downtown') {
                        locColor = '#e67e22';
                    }
                    locationBadge = '<span style="background: ' + locColor + '; color: ' + locTextColor + '; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-left: 8px;">' + appt.location.toUpperCase() + '</span>';
                }

				var editButton = appt.isFuture ? '<button class="btn btn-secondary" style="padding: 6px 10px; font-size: 12px;" onclick="event.stopPropagation(); editAppointment(\'' + appt.patient.patientID + '\')">Edit</button> <button class="btn" style="background: #e74c3c; color: white; padding: 6px 10px; font-size: 12px;" onclick="event.stopPropagation(); cancelAppointment(\'' + appt.patient.patientID + '\')">Cancel</button>' : '';
                var summaryText = appt.summary ? '<br><small style="color: ' + ((isCancelled || isRescheduled) ? '#e74c3c' : '#27ae60') + ';">✓ ' + appt.summary + '</small>' : '';
                
                return '<div class="appointment-item" style="background: ' + bgColor + '; border-left-color: ' + borderColor + ';">' +
                    '<div style="display: flex; justify-content: space-between; align-items: start;">' +
                    '<div onclick="viewPatientDetails(\'' + appt.patient.patientID + '\')" style="cursor: pointer; flex: 1;">' +
                    '<div class="appointment-time">' + (appt.time || 'Time TBD') + firstApptBadge + locationBadge + statusBadge + '</div>' +
                    '<div class="appointment-patient">' +
                    '<strong>' + formatNameWithAlias(appt.patient.patientName, appt.patient.patientAlias, appt.patient.patientFirstName, appt.patient.patientMiddleName, appt.patient.patientLastName) + '</strong>' +
					(appt.patient.isSurvivorshipClinic ? ' <span class="badge badge-survivorship">S</span>' : '') +
					(appt.patient.isOTC ? ' <span class="badge badge-otc">O</span>' : '') +
					' (' + appt.patient.patientID + ')' +
                    summaryText +
                    '</div></div>' + editButton +
                    '</div></div>';
            }).join('');

        }

        // Patient list rendering - SORTED BY NAME, LIMITED TO 5 WITH SCROLLING
        async function renderPatientList() {
            var container = document.getElementById('patientList');
            var searchTerm = document.getElementById('searchBox').value.toLowerCase();
            
            // Separate state filters from special filters
            var stateFilters = [];
            var specialFilters = [];
            
            for (var i = 0; i < currentFilter.length; i++) {
                var filter = currentFilter[i];
                if (filter === 'SURVIVORSHIP' || filter === 'OTC' || filter === 'PRIORITY' || filter === 'OVERDUE_APPOINTMENT') {
                    specialFilters.push(filter);
                } else {
                    // State filter
                    stateFilters.push(filter);
                }
            }
            
            // Get filtered patients from backend (SQL filtering!)
            var filtered;
            try {
                filtered = await eel.get_filtered_patients(
                    stateFilters.length > 0 ? stateFilters : null,
                    searchTerm || null,
                    specialFilters.length > 0 ? specialFilters : null
                )();
            } catch (error) {
                console.error('Error fetching filtered patients:', error);
                // Fallback to client-side filtering if backend fails
                filtered = patients;
            }
            
            // SORT based on currentSortMode
            filtered.sort(function(a, b) {
                if (currentSortMode === 'name') {
                    return a.patientName.localeCompare(b.patientName);
				} else if (currentSortMode === 'next-appt') {
					// Sort by next appointment date
					var dateA = a.nextAppointment || '';
					var dateB = b.nextAppointment || '';
					
					// Handle empty dates (no next appointment goes to end)
					if (!dateA && !dateB) return a.patientName.localeCompare(b.patientName);
					if (!dateA) return 1;
					if (!dateB) return -1;
					
					return dateB.localeCompare(dateA);  // Soonest first
				} else {
                    var dateA = getLastAppointmentDate(a);
                    var dateB = getLastAppointmentDate(b);
                    
                    // Handle null dates (patients with no appointment history)
                    if (!dateA && !dateB) return a.patientName.localeCompare(b.patientName);
                    if (!dateA) return 1;  // No history goes to end
                    if (!dateB) return -1;
                    
                    if (currentSortMode === 'appt-new') {
                        return dateB.localeCompare(dateA);  // Newest first
                    } else {
                        return dateA.localeCompare(dateB);  // Oldest first
                    }
                }
            });

            // Update patient count
            document.getElementById('patientCount').textContent = filtered.length;

            if (filtered.length === 0) {
                container.innerHTML = '<div class="empty-state">No patients found</div>';
                return;
            }

            // Render with Partner Name (PID) on the right only
            container.innerHTML = filtered.map(function(patient) {
                var state = STATES[patient.currentState];
                
                // Build badges
                var badges = '';
                if (patient.isSurvivorshipClinic) {
                    badges += '<span class="badge badge-survivorship">S</span>';
                }
                if (patient.isOTC) {
                    badges += '<span class="badge badge-otc">O</span>';
                }
                if (patient.isPriorityList) {
                    badges += '<span class="badge badge-priority">P</span>';
                }
                // Check if appointment is overdue
                var today = getTodayLocalDate(); // new Date().toISOString().split('T')[0];
                var isOverdue = (patient.currentState === 'WAITING_FIRST_APPT' || patient.currentState === 'WAITING_NEXT_APPT')
                                && patient.nextAppointment && patient.nextAppointment < today;
                if (isOverdue) {
                    badges += '<span class="badge" style="background: #5d6d7e; color: white;">OVERDUE</span>';
                }
                
                // Partner info for right side
                var partnerInfo = '';
                if (patient.partnerName && patient.partnerID) {
                    partnerInfo = formatNameWithAlias(patient.partnerName, patient.partnerAlias, patient.partnerFirstName, patient.partnerMiddleName, patient.partnerLastName) + ' (' + patient.partnerID + ')';
                } else if (patient.partnerID) {
                    partnerInfo = 'PID: ' + patient.partnerID;
                }
                
                return '<div class="patient-item" onclick="viewPatientDetails(\'' + patient.patientID + '\')">' +
                    '<div class="patient-left">' +
                    '<div>' +
                    '<span class="patient-name">' + formatNameWithAlias(patient.patientName, patient.patientAlias, patient.patientFirstName, patient.patientMiddleName, patient.patientLastName) + '</span>' +
                    '<span class="patient-id">(' + patient.patientID + ')</span>' +
                    '<span class="patient-status-badge" style="background: ' + state.color + '20; color: ' + state.color + ';">●&nbsp;</span>' +
                    '<span class="patient-status-inline" style="color: ' + state.color + ';">' + state.shortLabel + '</span>' +
                    (badges ? '<span class="patient-badges">' + badges + '</span>' : '') +
                    '</div>' +
                    '<div class="patient-info">' +
                    '📞 ' + (patient.patientPhone || patient.phone) + ' | 📧 ' + (patient.patientEmail || patient.email) +
                    '</div></div>' +
                    '<div class="patient-right">' + (partnerInfo || '') + '</div>' +
                    '</div>';
            }).join('');
        }

        function filterByStatus(status) {
            // Handle "All" button
            if (status === 'ALL') {
                currentFilter = [];
                // Update active buttons
                var buttons = document.querySelectorAll('.filter-btn');
                for (var i = 0; i < buttons.length; i++) {
                    buttons[i].classList.remove('active');
                }
                event.target.classList.add('active');
                renderPatientList();
                return;
            }
            
            // Toggle filter in array
            var index = currentFilter.indexOf(status);
            if (index > -1) {
                // Filter already active, remove it
                currentFilter.splice(index, 1);
                event.target.classList.remove('active');
            } else {
                // Filter not active, add it
                currentFilter.push(status);
                event.target.classList.add('active');
            }
            
            // Update "All" button state
            var buttons = document.querySelectorAll('.filter-btn');
            var allButton = buttons[0]; // First button is "All"
            if (currentFilter.length === 0) {
                // No filters active, activate "All"
                allButton.classList.add('active');
            } else {
                // Some filters active, deactivate "All"
                allButton.classList.remove('active');
            }
            
            renderPatientList();
        }

        // Search debounce timer
        var searchTimeout;
        
        // Debounced search handler
        function onSearchInput() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(async function() {
                await renderPatientList();
            }, 200);  // Wait 200ms after last keystroke
        }
        
        async function filterPatients() {
            await renderPatientList();
        }

        function setSortMode(mode) {
            currentSortMode = mode;
            
            // Update button states
            document.getElementById('sortName').classList.remove('active');
            document.getElementById('sortApptNew').classList.remove('active');
            document.getElementById('sortApptOld').classList.remove('active');
			document.getElementById('sortNextAppt').classList.remove('active');
            
            if (mode === 'name') {
                document.getElementById('sortName').classList.add('active');
            } else if (mode === 'appt-new') {
                document.getElementById('sortApptNew').classList.add('active');
            } else if (mode === 'appt-old') {
                document.getElementById('sortApptOld').classList.add('active');
            } else if (mode === 'next-appt') {
				document.getElementById('sortNextAppt').classList.add('active');
			}
            
            renderPatientList();
        }

        function getLastAppointmentDate(patient) {
            if (!patient.appointmentHistory || patient.appointmentHistory.length === 0) {
                return null;
            }
            
            var lastDate = null;
            for (var i = 0; i < patient.appointmentHistory.length; i++) {
                var apptDate = patient.appointmentHistory[i].date;
                if (!lastDate || apptDate > lastDate) {
                    lastDate = apptDate;
                }
            }
            return lastDate;
        }

        // Update status counts
        function updateStatusCounts(precomputedCounts) {
            var counts, overdueCount, priorityListCount;
            
            // If we have pre-computed counts from SQL (initial load), use them!
            if (precomputedCounts) {
                counts = {
                    WAITING_FIRST_APPT_SCHEDULE: precomputedCounts.state_counts['WAITING_FIRST_APPT_SCHEDULE'] || 0,
                    WAITING_FIRST_APPT: precomputedCounts.state_counts['WAITING_FIRST_APPT'] || 0,
                    WAITING_APPT_SUMMARY: precomputedCounts.state_counts['WAITING_APPT_SUMMARY'] || 0,
                    WAITING_NEXT_APPT_SCHEDULE: precomputedCounts.state_counts['WAITING_NEXT_APPT_SCHEDULE'] || 0,
                    WAITING_NEXT_APPT: precomputedCounts.state_counts['WAITING_NEXT_APPT'] || 0,
                    PREGNANT: precomputedCounts.state_counts['PREGNANT'] || 0,
                    INACTIVE: precomputedCounts.state_counts['INACTIVE'] || 0
                };
                overdueCount = precomputedCounts.overdue;
                priorityListCount = precomputedCounts.priority;
            } else {
                // Otherwise, calculate from loaded patients (after background load completes)
                counts = {
                    WAITING_FIRST_APPT_SCHEDULE: 0,
                    WAITING_FIRST_APPT: 0,
                    WAITING_APPT_SUMMARY: 0,
                    WAITING_NEXT_APPT_SCHEDULE: 0,
                    WAITING_NEXT_APPT: 0,
                    PREGNANT: 0,
                    INACTIVE: 0
                };
                
                var today = getTodayLocalDate();
                overdueCount = 0;
                priorityListCount = 0;
                
                // OPTIMIZED: Single loop instead of three separate loops (3x faster!)
                for (var i = 0; i < patients.length; i++) {
                    var p = patients[i];
                    
                    // Count by state
                    counts[p.currentState]++;
                    
                    // Count overdue appointments (while we're looping)
                    if ((p.currentState === 'WAITING_FIRST_APPT' || p.currentState === 'WAITING_NEXT_APPT')
                        && p.nextAppointment && p.nextAppointment < today) {
                        overdueCount++;
                    }
                    
                    // Count priority list patients (while we're looping)
                    if (p.isPriorityList === true) {
                        priorityListCount++;
                    }
                }
            }

            var container = document.getElementById('statusCounts');
            container.innerHTML = 
                '<div class="status-count-wide" style="border-color: ' + STATES.WAITING_FIRST_APPT_SCHEDULE.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.WAITING_FIRST_APPT_SCHEDULE.color + ';">' + counts.WAITING_FIRST_APPT_SCHEDULE + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.WAITING_FIRST_APPT_SCHEDULE.color + ';">Waiting 1st<br>Appointment Schedule</span></div>' +
                
                '<div class="status-count" style="border-color: ' + STATES.WAITING_FIRST_APPT.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.WAITING_FIRST_APPT.color + ';">' + counts.WAITING_FIRST_APPT + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.WAITING_FIRST_APPT.color + ';">Waiting 1st<br>Appointment</span></div>' +
                
                '<div class="status-count-wide" style="border-color: ' + STATES.WAITING_NEXT_APPT_SCHEDULE.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.WAITING_NEXT_APPT_SCHEDULE.color + ';">' + counts.WAITING_NEXT_APPT_SCHEDULE + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.WAITING_NEXT_APPT_SCHEDULE.color + ';">Waiting Next<br>Appointment Schedule</span></div>' +
                
                '<div class="status-count" style="border-color: ' + STATES.WAITING_NEXT_APPT.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.WAITING_NEXT_APPT.color + ';">' + counts.WAITING_NEXT_APPT + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.WAITING_NEXT_APPT.color + ';">Waiting Next<br>Appointment</span></div>' +
                
                '<div class="status-count" style="border-color: ' + STATES.OVERDUE_APPOINTMENT.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.OVERDUE_APPOINTMENT.color + ';">' + overdueCount + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.OVERDUE_APPOINTMENT.color + ';">Overdue<br>Appointments</span></div>' +

                '<div class="status-count" style="border-color: #9b59b6;">' +
                '<span class="status-count-number" style="color: #9b59b6;">' + priorityListCount  + '</span>' +
                '<span class="status-count-label" style="color: #9b59b6;">Priority<br>List</span></div>' +
                
                '<div class="status-count" style="border-color: ' + STATES.PREGNANT.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.PREGNANT.color + ';">' + counts.PREGNANT + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.PREGNANT.color + ';">Pregnant</span></div>' +
                
                '<div class="status-count" style="border-color: ' + STATES.INACTIVE.color + ';">' +
                '<span class="status-count-number" style="color: ' + STATES.INACTIVE.color + ';">' + counts.INACTIVE + '</span>' +
                '<span class="status-count-label" style="color: ' + STATES.INACTIVE.color + ';">Inactive</span></div>';
        }

        // Modal management
        function openAddPatientModal() {
            // Block in read-only mode
            if (isReadOnly) {
                showErrorModal('Cannot add patients - database is in read-only mode.\nAnother user (' + lockOwner + ') is currently editing.');
                return;
            }
            
            currentEditingPatient = null;
            document.getElementById('modalTitle').textContent = 'Add New Patient';
            document.getElementById('patientForm').reset();
            document.getElementById('patientID').disabled = false;
            document.getElementById('isSurvivorshipClinic').checked = false;
            document.getElementById('isOTC').checked = false;
            document.getElementById('isPriorityList').checked = false;
            document.getElementById('patientModal').classList.add('active');
        }

        function closeModal(modalId) {
            document.getElementById(modalId).classList.remove('active');
			// Hide day view panel when any modal closes
			if (modalId === 'editApptModal' || modalId === 'transitionModal') {
				document.getElementById('dayViewPanel').style.display = 'none';
			}			
        }

        function showError(message) {
            document.getElementById('errorModalMessage').textContent = message;
            document.getElementById('errorModal').classList.add('active');
        }

		// ============================================================================
        // WEEK VIEW FUNCTIONS
        // ============================================================================
        
        var currentWeekStart = null; // Monday of the current week being viewed
        
        // Go to today
        async function goToToday() {
            currentViewDate = new Date();
            updateDateDisplay();
            await renderAppointments();
            // Reload clinic data from DB
            await loadCurrentDayClinicData();
            updateClinicTypeButtons();
        }
        
		// Go to next clinic day
        async function goToNextClinic() {
            var currentDateStr = formatDateStr(currentViewDate);
            var nextClinicDate = null;
            
            // Try to find next clinic in already loaded data
            var allDates = Object.keys(clinicDays).sort();
            for (var i = 0; i < allDates.length; i++) {
                if (allDates[i] > currentDateStr) {
                    nextClinicDate = allDates[i];
                    break;
                }
            }
            
            // If not found, try loading next 3 months
            if (!nextClinicDate) {
                var searchDate = new Date(currentViewDate);
                for (var monthOffset = 1; monthOffset <= 3; monthOffset++) {
                    searchDate.setMonth(currentViewDate.getMonth() + monthOffset);
                    await loadMonthClinicDays(searchDate.getFullYear(), searchDate.getMonth() + 1);
                    
                    // Check again after loading
                    allDates = Object.keys(clinicDays).sort();
                    for (var i = 0; i < allDates.length; i++) {
                        if (allDates[i] > currentDateStr) {
                            nextClinicDate = allDates[i];
                            break;
                        }
                    }
                    
                    if (nextClinicDate) break;
                }
            }
            
            if (nextClinicDate) {
                var parts = nextClinicDate.split('-');
                currentViewDate = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
                updateDateDisplay();
                await renderAppointments();
                // Reload clinic data from DB
                await loadCurrentDayClinicData();
                updateClinicTypeButtons();
            } else {
                showErrorModal('No upcoming clinic days found in the next 3 months.');
            }
        }
        
        // Open week view modal
        async function openWeekViewModal() {
            // Set week start to Monday of the week containing currentViewDate
            currentWeekStart = getMonday(currentViewDate);
            
            // Load clinic days for the week (might span 2 months)
            var weekEnd = new Date(currentWeekStart);
            weekEnd.setDate(weekEnd.getDate() + 6);
            
            // Load both months if week spans two months
            await loadMonthClinicDays(currentWeekStart.getFullYear(), currentWeekStart.getMonth() + 1);
            if (weekEnd.getMonth() !== currentWeekStart.getMonth()) {
                await loadMonthClinicDays(weekEnd.getFullYear(), weekEnd.getMonth() + 1);
            }
            
            await renderWeekView();
            document.getElementById('weekViewModal').classList.add('active');
        }
        
		// Open portal access modal
		function openPortalModal() {
			loadPortalUsers();
		}
		
        // Get Monday of the week containing a date
        function getMonday(date) {
            var d = new Date(date);
            var day = d.getDay();
            var diff = d.getDate() - day + (day === 0 ? -6 : 1); // Adjust when day is Sunday
            return new Date(d.setDate(diff));
        }
        
        // Change week (direction: -1 for prev, 1 for next)
        async function changeWeek(direction) {
            currentWeekStart.setDate(currentWeekStart.getDate() + (direction * 7));
            
            // Load clinic days for the new week
            var weekEnd = new Date(currentWeekStart);
            weekEnd.setDate(weekEnd.getDate() + 6);
            
            // Load both months if week spans two months
            await loadMonthClinicDays(currentWeekStart.getFullYear(), currentWeekStart.getMonth() + 1);
            if (weekEnd.getMonth() !== currentWeekStart.getMonth()) {
                await loadMonthClinicDays(weekEnd.getFullYear(), weekEnd.getMonth() + 1);
            }
            
            await renderWeekView();
        }
        
        // Format date as YYYY-MM-DD
        function formatDateStr(date) {
            return date.getFullYear() + '-' +
                ('0' + (date.getMonth() + 1)).slice(-2) + '-' +
                ('0' + date.getDate()).slice(-2);
        }
        
		// Render the week view
        async function renderWeekView() {
            // TRIGGER 2: Smart refresh when opening weekly view (skip on initial load)
            if (typeof doSmartRefresh !== 'undefined') {
                await doSmartRefresh('open_weekly_view');
            }
            
            var today = getTodayLocalDate();
            
            // Start from Monday of current week and keep adding days until we fill the space
            var maxUnits = 7;
            var currentUnits = 0;
            var allDates = [];
            var dayIndex = 0;
            
            while (currentUnits < maxUnits) {
                var d = new Date(currentWeekStart);
                d.setDate(d.getDate() + dayIndex);
                var dateStr = formatDateStr(d);
                
                // Count appointments for this day
                var apptCount = 0;
                for (var p = 0; p < patients.length; p++) {
                    if (patients[p].nextAppointment === dateStr) apptCount++;
                }
                
                var hasAppointments = apptCount > 0;
                var dayUnits = hasAppointments ? 1 : 0.5;
                
                if (currentUnits + dayUnits > maxUnits && allDates.length >= 7) {
                    break;
                }
                
                var dayOfWeek = d.getDay();
                var dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
                
                allDates.push({
                    date: d,
                    dateStr: dateStr,
                    dayName: dayNames[dayOfWeek],
                    dayNum: d.getDate(),
                    month: d.getMonth() + 1,
                    isToday: dateStr === today,
                    isWeekend: dayOfWeek === 0 || dayOfWeek === 6,
                    hasAppointments: hasAppointments,
                    units: dayUnits
                });
                
                currentUnits += dayUnits;
                dayIndex++;
                
                if (dayIndex > 21) break;
            }
            
            // Update title
            var startDate = allDates[0].date;
            var endDate = allDates[allDates.length - 1].date;
            var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
            var title = months[startDate.getMonth()] + ' ' + startDate.getDate() + ' - ' + 
                        months[endDate.getMonth()] + ' ' + endDate.getDate() + ', ' + endDate.getFullYear();
            document.getElementById('weekViewTitle').textContent = title;
            
            // Get appointments for all dates
            var weekAppointments = getWeekAppointments(allDates);
            
            // Calculate total units for percentage widths
            var totalUnits = 0;
            for (var i = 0; i < allDates.length; i++) {
                totalUnits += allDates[i].units;
            }
            
            // Pre-calculate column widths
            var colWidths = [];
            for (var i = 0; i < allDates.length; i++) {
                colWidths.push((allDates[i].units / totalUnits) * 100);
            }
            
            // Build colgroup for both tables
            var colgroup = '<colgroup><col style="width:45px;">';
            for (var i = 0; i < allDates.length; i++) {
                colgroup += '<col style="width:' + colWidths[i] + '%;">';
            }
            colgroup += '</colgroup>';
            
            // Build HTML
            var html = '';
            

            // ===== HEADER TABLE (fixed) =====
            html += '<table style="width:calc(100% - 17px);border-spacing:0;table-layout:fixed;font-size:11px;border:2px solid #666;border-bottom:none;">';
            html += colgroup;
            html += '<tr>';
            html += '<th style="background:#f5f5f5;padding:8px 4px;border-right:2px solid #666;"></th>';
            
            for (var i = 0; i < allDates.length; i++) {
                var wd = allDates[i];
                var dayData = clinicDays[wd.dateStr] || {};
                var dayAppts = weekAppointments[wd.dateStr] || [];
                var apptCount = dayAppts.length;
                var firstApptCount = 0;
                for (var a = 0; a < dayAppts.length; a++) {
                    if (dayAppts[a].isFirstAppt) firstApptCount++;
                }
                
                var headerBg = wd.isToday ? '#e3f2fd' : (wd.isWeekend ? '#f0f0f0' : '#f5f5f5');
                var rightBorder = (i === allDates.length - 1) ? 'none' : '2px solid #666';
                
                html += '<th style="background:' + headerBg + ';padding:6px 4px;border-right:' + rightBorder + ';text-align:center;vertical-align:top;">';
                html += '<div style="font-size:10px;color:#666;">' + wd.dayName + '</div>';
                html += '<div style="font-size:13px;font-weight:700;">' + wd.dayNum + '/' + ('0' + wd.month).slice(-2) + '</div>';
                
                var hasClinicType = dayData.vaughan || dayData.downtown || dayData.ivf || dayData.md2 || dayData.survivorship;
                if (hasClinicType || apptCount > 0) {
                    html += '<div style="margin-top:3px;font-size:9px;">';
                    if (dayData.vaughan) html += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#f1c40f;color:#333;">V</span>';
                    if (dayData.downtown) html += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#e67e22;color:white;">DT</span>';
                    if (dayData.ivf) html += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#e74c3c;color:white;">IVF</span>';
                    if (dayData.md2) html += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#3498db;color:white;">MD2</span>';
                    if (dayData.survivorship) html += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#9b59b6;color:white;">S</span>';
                    
                    if (apptCount > 0) {
                        html += ' <span style="color:#888;">(';
                        if (firstApptCount > 0) {
                            html += '<span style="color:#9b59b6;font-weight:600;">' + firstApptCount + '</span>/';
                        }
                        html += apptCount;
                        if (firstApptCount > 0) html += ' 1st';
                        html += ')</span>';
                    }
                    html += '</div>';
                }
                
                html += '</th>';
            }
            html += '</tr></table>';
            
            // ===== BODY TABLE (scrollable) =====
			html += '<div style="border-top:2px solid #666;overflow-y:scroll;height:calc(100% - 70px);transform:translateZ(0);will-change:scroll-position;-webkit-overflow-scrolling:touch;">';
            html += '<table style="width:100%;border-spacing:0;table-layout:fixed;font-size:11px;border:2px solid #666;border-top:none;">';
            html += colgroup;
            
            // Time rows - only show hours, not 15-min slots
            for (var hour = 7; hour < 19; hour++) {
                var timeStr = ('0' + hour).slice(-2) + ':00';
                var borderTop = (hour === 7) ? 'none' : '1px solid #999';
                
                html += '<tr>';
                
                // Time cell - spans 4 rows visually but we'll use rowspan
                html += '<td rowspan="4" style="background:#f5f5f5;padding:2px 4px;text-align:right;border-right:2px solid #666;border-top:' + borderTop + ';font-size:10px;color:#666;vertical-align:top;height:80px;">' + timeStr + '</td>';
                
                // Day cells for :00
                for (var d = 0; d < allDates.length; d++) {
                    var wd = allDates[d];
                    html += buildDayCell(wd, hour, '00', d === allDates.length - 1, weekAppointments, borderTop);
                }
                html += '</tr>';
                
                // :15 row
                html += '<tr>';
                for (var d = 0; d < allDates.length; d++) {
                    var wd = allDates[d];
                    html += buildDayCell(wd, hour, '15', d === allDates.length - 1, weekAppointments, 'none');
                }
                html += '</tr>';
                
                // :30 row
                html += '<tr>';
                for (var d = 0; d < allDates.length; d++) {
                    var wd = allDates[d];
                    html += buildDayCell(wd, hour, '30', d === allDates.length - 1, weekAppointments, '1px solid #ddd');
                }
                html += '</tr>';
                
                // :45 row
                html += '<tr>';
                for (var d = 0; d < allDates.length; d++) {
                    var wd = allDates[d];
                    html += buildDayCell(wd, hour, '45', d === allDates.length - 1, weekAppointments, 'none');
                }
                html += '</tr>';
            }
            
            html += '</table></div>';
            
            document.getElementById('weekViewContent').innerHTML = html;
            
            // Helper function to build a day cell
            function buildDayCell(wd, hour, minutes, isLastCol, weekAppointments, borderTop) {
                var dayData = clinicDays[wd.dateStr] || {};
                var isMD2Day = dayData.md2 || false;
                var time = ('0' + hour).slice(-2) + ':' + minutes;
                
                // Get day of week for Tuesday admin time check
                var dayOfWeek = wd.date.getDay();
                
                // Check if greyed out (lunch break, MD2 break, Tuesday admin time)
                var isLunchBreak = (hour === 12);
                var isMD2Break = isMD2Day && (hour === 11 || hour === 12 || (hour === 13 && (minutes === '00' || minutes === '15')));
                var isTuesdayAdminTime = (dayOfWeek === 2) && (hour === 16);
                var isGreyedOut = isLunchBreak || isMD2Break || isTuesdayAdminTime;
                
                var cellBg;
                if (isGreyedOut) cellBg = '#e8e8e8';
                else if (wd.isToday) cellBg = '#e8f4fc';
                else if (wd.isWeekend) cellBg = '#f5f5f5';
                else cellBg = '#fff';
                
                var rightBorder = isLastCol ? 'none' : '1px solid #999';
                
                var cellHtml = '<td style="background:' + cellBg + ';border-right:' + rightBorder + ';border-top:' + borderTop + ';height:20px;padding:1px;position:relative;">';
                
                // Appointments at this time
                var dayAppts = weekAppointments[wd.dateStr] || [];
                var apptsAtTime = [];
                for (var a = 0; a < dayAppts.length; a++) {
                    if (dayAppts[a].time === time) {
                        apptsAtTime.push(dayAppts[a]);
                    }
                }
                
                if (apptsAtTime.length > 0) {
                    var widthPercent = 100 / apptsAtTime.length;
                    for (var a = 0; a < apptsAtTime.length; a++) {
                        var appt = apptsAtTime[a];
                        var leftPercent = a * widthPercent;
                        var heightPx = (appt.duration / 15) * 20 - 2;
                        
                        var bgColor, textColor;
                        if (appt.location === 'Virtual') { bgColor = '#b19cd9'; textColor = 'white'; }
                        else if (appt.location === 'Vaughan') { bgColor = '#f1c40f'; textColor = '#333'; }
                        else if (appt.location === 'Downtown') { bgColor = '#e67e22'; textColor = 'white'; }
                        else { bgColor = '#ccc'; textColor = '#333'; }
                        
                        var borderLeft = appt.isFirstAppt ? 'border-left:3px solid #9b59b6;' : '';
                        
                        cellHtml += '<div onclick="viewPatientDetailsFromWeekView(\'' + appt.patientID + '\')" ';
                        cellHtml += 'title="' + appt.time + ' - ' + appt.patientName + '" ';
                        cellHtml += 'style="position:absolute;top:1px;left:calc(' + leftPercent + '% + 1px);width:calc(' + widthPercent + '% - 3px);height:' + heightPx + 'px;';
                        cellHtml += 'background:' + bgColor + ';color:' + textColor + ';padding:2px 4px;border-radius:3px;font-size:10px;';
                        cellHtml += 'overflow:hidden;cursor:pointer;box-sizing:border-box;z-index:1;font-weight:600;white-space:nowrap;' + borderLeft + '">';
                        cellHtml += appt.patientName;
                        cellHtml += '</div>';
                    }
                }
                
                cellHtml += '</td>';
                return cellHtml;
            }
        }

		// View patient details from week view (ensures modal appears on top)
        async function viewPatientDetailsFromWeekView(patientID) {
            // Close week view first, then open patient details
            // Or use z-index to ensure details modal is on top
            var weekModal = document.getElementById('weekViewModal');
            var detailsModal = document.getElementById('detailsModal');
            
            // Temporarily increase z-index of details modal
            if (detailsModal) {
                detailsModal.style.zIndex = '1001';
            }
            
            await viewPatientDetails(patientID);
        }
        
		// Get appointments for a week
        function getWeekAppointments(weekDates) {
            var result = {};
            
            // Initialize empty arrays for each day
            for (var i = 0; i < weekDates.length; i++) {
                result[weekDates[i].dateStr] = [];
            }
            
            // Collect appointments from patients
            for (var i = 0; i < patients.length; i++) {
                var p = patients[i];
                if (!p.nextAppointment || !p.appointmentTime) continue;
                
                // Check if appointment is in this week
                if (result[p.nextAppointment] !== undefined) {
                    // Check if this is a first appointment
					var isFirstAppt = patients[i].currentState === 'WAITING_FIRST_APPT';
// FIXME: Remove once confirmed
//                    var isFirstAppt = !p.appointmentHistory || p.appointmentHistory.length === 0 ||
//                        p.appointmentHistory.every(function(hist) { return hist.date === p.nextAppointment; });
                    
                    // Build patient display name (First Last only)
                    var patientDisplay = '';
                    if (p.patientFirstName && p.patientLastName) {
                        patientDisplay = p.patientFirstName + ' ' + p.patientLastName;
                    } else if (p.patientName) {
                        patientDisplay = p.patientName;
                    }
                    
                    var partnerDisplay = '';
                    if (p.partnerFirstName && p.partnerLastName) {
                        partnerDisplay = p.partnerFirstName + ' ' + p.partnerLastName;
                    } else if (p.partnerName) {
                        partnerDisplay = p.partnerName;
                    }
                    
                    result[p.nextAppointment].push({
                        patientID: p.patientID,
                        patientName: patientDisplay,
                        partnerName: partnerDisplay,
                        time: p.appointmentTime,
                        location: p.appointmentLocation || '',
                        isFirstAppt: isFirstAppt,
                        duration: 30 // Default, will be adjusted below
                    });
                }
            }
            
            // Sort each day's appointments by time and calculate durations 
            for (var dateStr in result) {
                result[dateStr].sort(function(a, b) {
                    return a.time.localeCompare(b.time);
                });
                
                // Adjust duration based on next appointment and time slot
                for (var i = 0; i < result[dateStr].length; i++) {
                    var appt = result[dateStr][i];
                    var timeParts = appt.time.split(':');
					var apptMinutes = parseInt(timeParts[0]) * 60 + parseInt(timeParts[1]);
					var minutes = parseInt(timeParts[1]);
                    
				    // Check if appointment starts at :15 or :45 (odd 15-min slots)
					var startsAtOddSlot = (minutes === 15 || minutes === 45);
	
                    // Check if there's another appointment within the next 30 minutes
                    var nextApptIn15 = false;
                    for (var j = 0; j < result[dateStr].length; j++) {
                        if (i === j) continue;
                        var otherMinutes = parseInt(result[dateStr][j].time.split(':')[0]) * 60 + parseInt(result[dateStr][j].time.split(':')[1]);
                        var diff = otherMinutes - apptMinutes;
                        if (diff === 15) {
                            nextApptIn15 = true;
                            break;
                        }
                    }
                    
					// Duration is 15 min if: starts at odd slot OR has another appt 15 min later
                    appt.duration = (startsAtOddSlot || nextApptIn15) ? 15 : 30;
                }
            }
            
            return result;
        }

		// Render day view for appointment scheduling
		function updateDayViewPanel(dateStr) {
			if (!dateStr) {
				document.getElementById('dayViewPanelContent').innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">Select a date to view appointments</div>';
				return;
			}
			
			var panelContent = document.getElementById('dayViewPanelContent');
			var panelTitle = document.getElementById('dayViewPanelTitle');
			
			// Format date for title
			var d = new Date(dateStr + 'T12:00:00');
			var dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
			var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

			// Get day data for clinic types
			var dayData = clinicDays[dateStr] || {};
			var titleHtml = '<div>' + dayNames[d.getDay()] + ', ' + months[d.getMonth()] + ' ' + d.getDate() + '</div>';

			// Add clinic type badges
			var hasClinicType = dayData.vaughan || dayData.downtown || dayData.ivf || dayData.md2 || dayData.survivorship;
			if (hasClinicType) {
				titleHtml += '<div style="margin-top:4px;font-size:9px;">';
				if (dayData.vaughan) titleHtml += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#f1c40f;color:#333;">V</span>';
				if (dayData.downtown) titleHtml += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#e67e22;color:white;">DT</span>';
				if (dayData.ivf) titleHtml += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#e74c3c;color:white;">IVF</span>';
				if (dayData.md2) titleHtml += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#3498db;color:white;">MD2</span>';
				if (dayData.survivorship) titleHtml += '<span style="display:inline-block;padding:1px 4px;border-radius:3px;margin:1px;background:#9b59b6;color:white;">S</span>';
				titleHtml += '</div>';
			}

			panelTitle.innerHTML = titleHtml;
			
			// Get appointments for this date
			var dayAppointments = [];
			for (var i = 0; i < patients.length; i++) {
				if (patients[i].nextAppointment === dateStr) {
					var time = patients[i].appointmentTime || '09:00';
					var duration = patients[i].appointmentDuration || 30;
					var location = patients[i].appointmentLocation || '';
					var isFirstAppt = patients[i].currentState === 'WAITING_FIRST_APPT';
					
					dayAppointments.push({
						time: time,
						duration: duration,
						location: location,
						patientName: patients[i].patientName,
						patientID: patients[i].patientID,
						isFirstAppt: isFirstAppt
					});
				}
			}
			
			// Sort by time
			dayAppointments.sort(function(a, b) {
				return a.time.localeCompare(b.time);
			});
			
			// Adjust duration based on next appointment and time slot
			for (var i = 0; i < dayAppointments.length; i++) {
				var appt = dayAppointments[i];
				var timeParts = appt.time.split(':');
				var apptMinutes = parseInt(timeParts[0]) * 60 + parseInt(timeParts[1]);
				var minutes = parseInt(timeParts[1]);
				
				// Check if appointment starts at :15 or :45 (odd 15-min slots)
				var startsAtOddSlot = (minutes === 15 || minutes === 45);
				
				// Check if there's another appointment 15 minutes later
				var nextApptIn15 = false;
				for (var j = 0; j < dayAppointments.length; j++) {
					if (i === j) continue;
					var otherTimeParts = dayAppointments[j].time.split(':');
					var otherMinutes = parseInt(otherTimeParts[0]) * 60 + parseInt(otherTimeParts[1]);
					var diff = otherMinutes - apptMinutes;
					if (diff === 15) {
						nextApptIn15 = true;
						break;
					}
				}
				
				// Duration is 15 min if: starts at odd slot OR has another appt 15 min later
				appt.duration = (startsAtOddSlot || nextApptIn15) ? 15 : 30;
			}
			
			// Build the Outlook-style table
			var html = '<table style="width:100%;border-spacing:0;table-layout:fixed;font-size:11px;border:2px solid #666;">';
			html += '<colgroup><col style="width:45px;"><col></colgroup>';
			
			// Time rows
			for (var hour = 7; hour < 19; hour++) {
				var timeStr = ('0' + hour).slice(-2) + ':00';
				var borderTop = (hour === 7) ? 'none' : '1px solid #999';
				
				html += '<tr>';
				html += '<td rowspan="4" style="background:#f5f5f5;padding:2px 4px;text-align:right;border-right:2px solid #666;border-top:' + borderTop + ';font-size:10px;color:#666;vertical-align:top;height:80px;">' + timeStr + '</td>';
				html += buildDayViewCell(hour, '00', dayAppointments, borderTop, dateStr, d.getDay());
				html += '</tr>';
				
				html += '<tr>' + buildDayViewCell(hour, '15', dayAppointments, 'none', dateStr, d.getDay()) + '</tr>';
				html += '<tr>' + buildDayViewCell(hour, '30', dayAppointments, '1px solid #ddd', dateStr, d.getDay()) + '</tr>';
				html += '<tr>' + buildDayViewCell(hour, '45', dayAppointments, 'none', dateStr, d.getDay()) + '</tr>';
			}
			
			html += '</table>';
			panelContent.innerHTML = html;
		}

		// Helper function to build day view cells
		function buildDayViewCell(hour, minutes, dayAppointments, borderTop, dateStr, dayOfWeek) {
			var time = ('0' + hour).slice(-2) + ':' + minutes;
			
			// Check if greyed out (lunch break, MD2 break, Tuesday admin time)
			var dayData = clinicDays[dateStr] || {};
			var isMD2Day = dayData.md2 || false;

			var isLunchBreak = (hour === 12);
			var isMD2Break = isMD2Day && (hour === 11 || (hour === 13 && (minutes === '00' || minutes === '15')));
			var isTuesdayAdminTime = (dayOfWeek === 2) && (hour === 16);
			var isGreyedOut = isLunchBreak || isMD2Break || isTuesdayAdminTime;

			var cellBg = isGreyedOut ? '#e8e8e8' : '#fff';
			
			var cellHtml = '<td style="background:' + cellBg + ';border-top:' + borderTop + ';height:20px;padding:1px;position:relative;">';
			
			// Find appointments at this time
			var apptsAtTime = [];
			for (var a = 0; a < dayAppointments.length; a++) {
				if (dayAppointments[a].time === time) {
					apptsAtTime.push(dayAppointments[a]);
				}
			}
			
			if (apptsAtTime.length > 0) {
				var widthPercent = 100 / apptsAtTime.length;
				for (var a = 0; a < apptsAtTime.length; a++) {
					var appt = apptsAtTime[a];
					var leftPercent = a * widthPercent;
					var heightPx = (appt.duration / 15) * 20 - 2;
					
					var bgColor, textColor;
					if (appt.location === 'Virtual') { bgColor = '#b19cd9'; textColor = 'white'; }
					else if (appt.location === 'Vaughan') { bgColor = '#f1c40f'; textColor = '#333'; }
					else if (appt.location === 'Downtown') { bgColor = '#e67e22'; textColor = 'white'; }
					else { bgColor = '#ccc'; textColor = '#333'; }
					
					var borderLeft = appt.isFirstAppt ? 'border-left:3px solid #9b59b6;' : '';
					
					cellHtml += '<div title="' + appt.time + ' - ' + appt.patientName + '" ';
					cellHtml += 'style="position:absolute;top:1px;left:calc(' + leftPercent + '% + 1px);width:calc(' + widthPercent + '% - 3px);height:' + heightPx + 'px;';
					cellHtml += 'background:' + bgColor + ';color:' + textColor + ';padding:2px 4px;border-radius:3px;font-size:10px;';
					cellHtml += 'overflow:hidden;box-sizing:border-box;z-index:1;font-weight:600;white-space:nowrap;' + borderLeft + '">';
					cellHtml += appt.patientName;
					cellHtml += '</div>';
				}
			}
			
			cellHtml += '</td>';
			return cellHtml;
		}

        function switchTab(tabName) {
            var tabs = document.querySelectorAll('.tab');
            var contents = document.querySelectorAll('.tab-content');
            
            for (var i = 0; i < tabs.length; i++) {
                tabs[i].classList.remove('active');
            }
            for (var i = 0; i < contents.length; i++) {
                contents[i].classList.remove('active');
            }
            
            event.target.classList.add('active');
            document.getElementById(tabName + 'Tab').classList.add('active');
        }

        // Patient management
        async function savePatient(event) {
            event.preventDefault();
            
		var patientData = {
                patientID: document.getElementById('patientID').value,
                // Patient name fields
                patientFirstName: document.getElementById('patientFirstName').value,
                patientMiddleName: document.getElementById('patientMiddleName').value,
                patientLastName: document.getElementById('patientLastName').value,
                patientAlias: document.getElementById('patientAlias').value,
                patientName: buildFullName(
                    document.getElementById('patientFirstName').value,
                    document.getElementById('patientMiddleName').value,
                    document.getElementById('patientLastName').value
                ),
                // Partner fields
                partnerID: document.getElementById('partnerID').value,
                partnerFirstName: document.getElementById('partnerFirstName').value,
                partnerMiddleName: document.getElementById('partnerMiddleName').value,
                partnerLastName: document.getElementById('partnerLastName').value,
                partnerAlias: document.getElementById('partnerAlias').value,
                partnerName: buildFullName(
                    document.getElementById('partnerFirstName').value,
                    document.getElementById('partnerMiddleName').value,
                    document.getElementById('partnerLastName').value
                ),
                // Contact info
                patientPhone: document.getElementById('patientPhone').value,
                patientEmail: document.getElementById('patientEmail').value,
                partnerPhone: document.getElementById('partnerPhone').value,
                partnerEmail: document.getElementById('partnerEmail').value,
                notes: document.getElementById('notes').value,
                isSurvivorshipClinic: document.getElementById('isSurvivorshipClinic').checked,
                isOTC: document.getElementById('isOTC').checked,
                isPriorityList: document.getElementById('isPriorityList').checked
            };
			
            if (currentEditingPatient) {
                // Update existing patient
                var success = await eel.update_patient(currentEditingPatient.patientID, patientData)();
                
                if (success) {
                    // Add notes to history if changed
                    if (patientData.notes !== currentEditingPatient.notes && patientData.notes) {
                        await eel.add_note_history(currentEditingPatient.patientID, patientData.notes)();
                    }
                    
                    // Refresh ONLY this patient from backend to get updated history
                    var updatedPatient = await eel.get_patient(currentEditingPatient.patientID)();
                    
                    // Update in local array
                    for (var i = 0; i < patients.length; i++) {
                        if (patients[i].patientID === currentEditingPatient.patientID) {
                            patients[i] = updatedPatient;
                            break;
                        }
                    }
                    
                    closeModal('patientModal');
                    
                    // Re-render affected views
                    renderPatientList();
                    renderAppointments(); // In case appointment info changed
                    updateStatusCounts(); // Update KPIs (in case isPriorityList changed)
                    
                    // Reopen the patient view
                    viewPatientDetails(patientData.patientID);
                } else {
                    showError('Failed to update patient in database');
                }
            } else {
                // Create new patient
                
                // Check for duplicate patient ID
                var existingPatient = patients.find(p => p.patientID === patientData.patientID);
                if (existingPatient) {
                    showErrorModal('A patient with ID "' + patientData.patientID + '" already exists.\n\nPlease use a different patient ID.');
                    return;
                }
                
                var newPatient = mergeObjects({}, patientData, {
                    dateAdded: new Date().toISOString().split('T')[0],
                    currentState: 'WAITING_FIRST_APPT_SCHEDULE',
                    stateHistory: [{
                        state: 'WAITING_FIRST_APPT_SCHEDULE',
                        timestamp: new Date().toISOString()
                    }],
                    nextAppointment: null,
                    appointmentTime: null,
                    appointmentHistory: [],
                    notesHistory: []
                });
                
                var success = await eel.add_patient(newPatient)();
                
                if (success) {
                    // Add initial note to history if provided
                    if (patientData.notes) {
                        await eel.add_note_history(patientData.patientID, patientData.notes)();
                    }
                    
                    // Fetch the complete patient from backend (with history)
                    var savedPatient = await eel.get_patient(patientData.patientID)();
                    
                    // Add to local array
                    patients.push(savedPatient);
                    
                    closeModal('patientModal');
                    
                    // Re-render affected views
                    renderPatientList();
                    renderAppointments();
                    
                    // Update ONLY the affected KPI (WAITING_FIRST_APPT_SCHEDULE +1)
                    updateStatusCounts();
                } else {
                    showError('Failed to add patient to database');
                }
            }
		}

        async function viewPatientDetails(patientID) {
            // TRIGGER 3: Smart refresh when opening patient details (skip on initial load)
            if (typeof doSmartRefresh !== 'undefined') {
                await doSmartRefresh('open_patient_details');
            }
            
            startTiming('viewPatientDetails');
            
            startTiming('find_patient');
            var patient = null;
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patient = patients[i];
                    break;
                }
            }
            endTiming('find_patient');
            
            if (!patient) {
                endTiming('viewPatientDetails');
                return;
            }

            currentEditingPatient = patient;
            currentViewingPatientID = patientID; // Store for email generator
            
            var state = STATES[patient.currentState];
            
            startTiming('build_badges');
            // Build badges for title
            var badges = '';
            if (patient.isSurvivorshipClinic) {
                badges += ' <span class="badge badge-survivorship">Survivorship Clinic</span>';
            }
            if (patient.isOTC) {
                badges += ' <span class="badge badge-otc">OTC</span>';
            }
            if (patient.isPriorityList) {
                badges += ' <span class="badge badge-priority">Priority List</span>';
            }
            endTiming('build_badges');
            
            startTiming('set_patient_name');
            document.getElementById('detailsPatientName').innerHTML = formatNameWithAlias(patient.patientName, patient.patientAlias, patient.patientFirstName, patient.patientMiddleName, patient.patientLastName) + ' (' + patient.patientID + ')' + badges;
            endTiming('set_patient_name');
            
            startTiming('build_partner_info');
            // Information tab - removed Patient ID, Name, and Categories rows
            var partnerInfo = '';
			if (patient.partnerName && patient.partnerID) {
                partnerInfo = formatNameWithAlias(patient.partnerName, patient.partnerAlias, patient.partnerFirstName, patient.partnerMiddleName, patient.partnerLastName) + ' (' + patient.partnerID + ')';
            } else if (patient.partnerID) {
                partnerInfo = patient.partnerID;
            } else if (patient.partnerName) {
                partnerInfo = formatNameWithAlias(patient.partnerName, patient.partnerAlias, patient.partnerFirstName, patient.partnerMiddleName, patient.partnerLastName);			} else {
                partnerInfo = '-';
            }
            endTiming('build_partner_info');
            
            var details = '<div class="details-row"><div class="details-label">Status:</div><div class="details-value" style="color: ' + state.color + '; font-weight: 600;">' + state.label + '</div></div>' +
                '<div class="details-row"><div class="details-label">Partner:</div><div class="details-value">' + partnerInfo + '</div></div>' +
                '<div class="details-row"><div class="details-label">Patient Phone:</div><div class="details-value">' + (patient.patientPhone || patient.phone) + '</div></div>' +
                '<div class="details-row"><div class="details-label">Patient Email:</div><div class="details-value">' + (patient.patientEmail || patient.email) + '</div></div>' +
                '<div class="details-row"><div class="details-label">Date Added:</div><div class="details-value">' + patient.dateAdded + '</div></div>';
            
			if (patient.nextAppointment) {
				var apptButtons = '';
				var today = getTodayLocalDate();
				if (patient.nextAppointment >= today) {
					apptButtons = ' <button class="btn btn-small btn-secondary" onclick="editAppointment(\'' + patient.patientID + '\')" style="margin-left: 10px;">Edit</button>' +
								  '<button class="btn btn-small" style="background: #e74c3c; color: white; margin-left: 5px;" onclick="cancelAppointment(\'' + patient.patientID + '\')">Cancel</button>';
				}
				var locationDisplay = patient.appointmentLocation ? ' (' + patient.appointmentLocation + ')' : '';
				details += '<div class="details-row"><div class="details-label">Next Appointment:</div><div class="details-value"><strong>' + patient.nextAppointment + '</strong>' +
					(patient.appointmentTime ? ' at ' + patient.appointmentTime : '') + locationDisplay + apptButtons + '</div></div>';
			}
            
            if (patient.notes) {
                details += '<div class="details-row"><div class="details-label">Notes:</div><div class="details-value">' + patient.notes + '</div></div>';
            }
            
            document.getElementById('patientDetails').innerHTML = details;
            
            startTiming('build_appointment_history');
            // Appointments tab
            var apptHistory = '<h3 style="margin-bottom: 15px; font-size: 16px;">Appointment History</h3>';
            if (patient.appointmentHistory && patient.appointmentHistory.length > 0) {
                apptHistory += '<div style="max-height: 300px; overflow-y: auto;">';
                apptHistory += patient.appointmentHistory.map(function(appt) {
                    return '<div class="history-item">' +
                        '<div class="history-timestamp">' + new Date(appt.timestamp).toLocaleString('en-US', {timeZone: 'America/Toronto'}) + '</div>' +
                        '<strong>Date:</strong> ' + appt.date + (appt.time ? ' at ' + appt.time : '') + '<br>' +
                        (appt.summary ? '<strong>Summary:</strong> ' + appt.summary : '') +
                        '</div>';
                }).join('');
                apptHistory += '</div>';
            } else {
                apptHistory += '<div class="empty-state">No appointment history yet</div>';
            }
            document.getElementById('appointmentHistory').innerHTML = apptHistory;
            endTiming('build_appointment_history');
            
            startTiming('build_state_history');
            // History tab
            var stateHistory = '<h3 style="margin-bottom: 15px; font-size: 16px;">State History</h3>';
            if (patient.stateHistory && patient.stateHistory.length > 0) {
                stateHistory += '<div style="max-height: 300px; overflow-y: auto;">';
                stateHistory += patient.stateHistory.map(function(hist) {
                    var histState = STATES[hist.state];
                    return '<div class="history-item" style="border-left-color: ' + histState.color + ';">' +
                        '<div class="history-timestamp">' + new Date(hist.timestamp).toLocaleString('en-US', {timeZone: 'America/Toronto'}) + '</div>' +
                        '<span style="color: ' + histState.color + '; font-weight: 600;">' + histState.label + '</span>' +
                        '</div>';
                }).join('');
                stateHistory += '</div>';
            }
            document.getElementById('stateHistoryContent').innerHTML = stateHistory;
            endTiming('build_state_history');
            
            var notesHistory = '';
            if (patient.notesHistory && patient.notesHistory.length > 0) {
                notesHistory = '<div style="max-height: 300px; overflow-y: auto;">';
                notesHistory += patient.notesHistory.map(function(note) {
                    return '<div class="history-item">' +
                        '<div class="history-timestamp">' + new Date(note.timestamp).toLocaleString('en-US', {timeZone: 'America/Toronto'}) + '</div>' +
                        note.note +
                        '</div>';
                }).join('');
                notesHistory += '</div>';
            } else {
                notesHistory = '<div class="empty-state">No notes history yet</div>';
            }
            document.getElementById('notesHistoryContent').innerHTML = notesHistory;
            
            // State transition buttons - hide in read-only mode
            var buttons = '';
            if (isReadOnly) {
                buttons = '<div style="color: #856404; background: #fff3cd; padding: 10px; border-radius: 4px; font-size: 12px;">Read-only mode - actions disabled</div>';
            } else if (patient.currentState === 'WAITING_FIRST_APPT_SCHEDULE') {
				buttons = '<div style="display: flex; justify-content: space-between; align-items: center;">';
				buttons += '<button class="btn btn-success" onclick="initiateStateTransition(\'' + patient.patientID + '\', \'WAITING_FIRST_APPT\')">Schedule First Appointment</button>';
				buttons += '<button class="btn btn-primary" onclick="openFreeSlotsModal()" style="padding: 5px 10px; font-size: 11px;">Free Slots</button>';
				buttons += '</div>';
                buttons += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e0e0e0;">';
                buttons += '<button class="btn btn-secondary" style="background: #e91e63; margin-right: 8px;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'PREGNANT\')">Mark as Pregnant</button>';
                buttons += '<button class="btn btn-secondary" style="background: #95a5a6;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'INACTIVE\')">Mark as Inactive</button>';
                buttons += '</div>';
            } else if (patient.currentState === 'WAITING_FIRST_APPT' || patient.currentState === 'WAITING_NEXT_APPT') {
                buttons = '<button class="btn btn-success" onclick="initiateStateTransition(\'' + patient.patientID + '\', \'WAITING_APPT_SUMMARY\')">Appointment Completed</button> ';
                buttons += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e0e0e0;">';
                buttons += '<button class="btn btn-secondary" style="background: #e91e63; margin-right: 8px;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'PREGNANT\')">Mark as Pregnant</button>';
                buttons += '<button class="btn btn-secondary" style="background: #95a5a6;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'INACTIVE\')">Mark as Inactive</button>';
                buttons += '</div>';
            } else if (patient.currentState === 'WAITING_APPT_SUMMARY') {
                buttons = '<button class="btn btn-success" onclick="initiateStateTransition(\'' + patient.patientID + '\', \'WAITING_NEXT_APPT_SCHEDULE\')">Summary Complete</button> ';
                buttons += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e0e0e0;">';
                buttons += '<button class="btn btn-secondary" style="background: #e91e63; margin-right: 8px;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'PREGNANT\')">Mark as Pregnant</button>';
                buttons += '<button class="btn btn-secondary" style="background: #95a5a6;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'INACTIVE\')">Mark as Inactive</button>';
                buttons += '</div>';
            } else if (patient.currentState === 'WAITING_NEXT_APPT_SCHEDULE') {
				buttons = '<div style="display: flex; justify-content: space-between; align-items: center;">';
				buttons += '<button class="btn btn-success" onclick="initiateStateTransition(\'' + patient.patientID + '\', \'WAITING_NEXT_APPT\')">Schedule Next Appointment</button>';
				buttons += '<button class="btn btn-primary" onclick="openFreeSlotsModal()" style="padding: 5px 10px; font-size: 11px;">Free Slots</button>';
				buttons += '</div>';
                buttons += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e0e0e0;">';
                buttons += '<button class="btn btn-secondary" style="background: #e91e63; margin-right: 8px;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'PREGNANT\')">Mark as Pregnant</button>';
                buttons += '<button class="btn btn-secondary" style="background: #95a5a6;" onclick="transitionToSpecialState(\'' + patient.patientID + '\', \'INACTIVE\')">Mark as Inactive</button>';
                buttons += '</div>';
            } else if (patient.currentState === 'PREGNANT' || patient.currentState === 'INACTIVE') {
                buttons = '<button class="btn btn-success" onclick="transitionFromSpecialState(\'' + patient.patientID + '\', \'' + patient.currentState + '\')">Return to Active Status</button>';
            }
            
            document.getElementById('stateTransitionButtons').innerHTML = buttons;
            
            // Show modal
            document.getElementById('detailsModal').classList.add('active');
            
            // Switch to info tab by default
            startTiming('switch_to_info_tab');
            var tabs = document.querySelectorAll('.tab');
            var contents = document.querySelectorAll('.tab-content');
            for (var i = 0; i < tabs.length; i++) {
                tabs[i].classList.remove('active');
            }
            for (var i = 0; i < contents.length; i++) {
                contents[i].classList.remove('active');
            }
            tabs[0].classList.add('active');
            document.getElementById('infoTab').classList.add('active');
            endTiming('switch_to_info_tab');
            
            endTiming('viewPatientDetails');
        }

        // Transition to Pregnant or Inactive state
        async function transitionToSpecialState(patientID, specialState) {
            if (isReadOnly) {
                showErrorModal('Cannot modify patients - database is in read-only mode.');
                return;
            }
            
            var message = 'Are you sure you want to mark this patient as ' + (specialState === 'PREGNANT' ? 'Pregnant' : 'Inactive') + '?';
            showConfirm(message, 'Confirm', async function(confirmed) {
                if (!confirmed) return;
                
                // Lock to prevent auto-refresh collision
                isManualOperationInProgress = true;
                startTiming('transitionToSpecialState');
                
                try {
                    // Save to backend (now optimized - only loads this one patient!)
                    startTiming('updatePatientStateWithSave');
                    await updatePatientStateWithSave(patientID, specialState, null);
                    endTiming('updatePatientStateWithSave');
                    
                    closeModal('detailsModal');
                    
                    startTiming('renderPatientList');
                    renderPatientList();
                    endTiming('renderPatientList');
                    
                    startTiming('renderAppointments');
                    await renderAppointments();
                    endTiming('renderAppointments');
                    
                    startTiming('updateStatusCounts');
                    updateStatusCounts();
                    endTiming('updateStatusCounts');
                    
                    endTiming('transitionToSpecialState');
                } finally {
                    // Always unlock, even if error
                    isManualOperationInProgress = false;
                }
            });
        }

        // Transition from Pregnant or Inactive back to normal flow
        async function transitionFromSpecialState(patientID, fromState) {
            if (isReadOnly) {
                showErrorModal('Cannot modify patients - database is in read-only mode.');
                return;
            }
            
            showConfirm('Return this patient to active status?', 'Confirm', async function(confirmed) {
                if (!confirmed) return;
                
                // Lock to prevent auto-refresh collision
                isManualOperationInProgress = true;
                
                try {
                    // Find the patient to check if they ever had a first appointment
                    var patient = null;
                    for (var i = 0; i < patients.length; i++) {
                        if (patients[i].patientID === patientID) {
                            patient = patients[i];
                            break;
                        }
                    }
                    
                    if (!patient) {
                        showErrorModal('Patient not found');
                        return;
                    }
                    
                    // Check if patient ever COMPLETED first appointment by looking at state history
                    // Only consider them as having had first appointment if they reached:
                    // - WAITING_APPT_SUMMARY (completed appointment)
                    // - WAITING_NEXT_APPT_SCHEDULE (after completing first appointment)
                    // - WAITING_NEXT_APPT (scheduled next appointment)
                    var completedFirstAppointment = false;
                    if (patient.stateHistory && patient.stateHistory.length > 0) {
                        for (var i = 0; i < patient.stateHistory.length; i++) {
                            var state = patient.stateHistory[i].state;
                            if (state === 'WAITING_APPT_SUMMARY' || 
                                state === 'WAITING_NEXT_APPT_SCHEDULE' || 
                                state === 'WAITING_NEXT_APPT') {
                                completedFirstAppointment = true;
                                break;
                            }
                        }
                    }
                    
                    // Determine correct state to return to
                    var newState = completedFirstAppointment ? 'WAITING_NEXT_APPT_SCHEDULE' : 'WAITING_FIRST_APPT_SCHEDULE';
                    
                    // Save to backend (already updates local patient!)
                    await updatePatientStateWithSave(patientID, newState, null);
                    
                    // Don't close modal - just refresh the patient details to show new state
                    viewPatientDetails(patientID);  // Refresh patient details modal
                    renderPatientList();
                    renderAppointments();
                    updateStatusCounts();
                } finally {
                    // Always unlock, even if error
                    isManualOperationInProgress = false;
                }
            });
        }

        function editCurrentPatient() {
            if (!currentEditingPatient) return;
            
            // Block in read-only mode
            if (isReadOnly) {
                showErrorModal('Cannot edit patients - database is in read-only mode.\nAnother user (' + lockOwner + ') is currently editing.');
                return;
            }
            
            closeModal('detailsModal');
            
            document.getElementById('modalTitle').textContent = 'Edit Patient';
			document.getElementById('patientID').value = currentEditingPatient.patientID;
            document.getElementById('patientID').disabled = true;
            // Patient name fields - fall back to full name for backward compatibility
            if (currentEditingPatient.patientFirstName || currentEditingPatient.patientLastName) {
                document.getElementById('patientFirstName').value = currentEditingPatient.patientFirstName || '';
                document.getElementById('patientMiddleName').value = currentEditingPatient.patientMiddleName || '';
                document.getElementById('patientLastName').value = currentEditingPatient.patientLastName || '';
            } else if (currentEditingPatient.patientName) {
                // Old patient - split full name: everything but last word = first, last word = last
                var parts = currentEditingPatient.patientName.trim().split(/\s+/);
                if (parts.length === 1) {
                    document.getElementById('patientFirstName').value = parts[0];
                    document.getElementById('patientLastName').value = '';
                } else {
                    document.getElementById('patientLastName').value = parts.pop();
                    document.getElementById('patientFirstName').value = parts.join(' ');
                }
                document.getElementById('patientMiddleName').value = '';
            } else {
                document.getElementById('patientFirstName').value = '';
                document.getElementById('patientMiddleName').value = '';
                document.getElementById('patientLastName').value = '';
            }
            document.getElementById('patientAlias').value = currentEditingPatient.patientAlias || '';
            // Partner fields - fall back to full name for backward compatibility
            document.getElementById('partnerID').value = currentEditingPatient.partnerID || '';
            if (currentEditingPatient.partnerFirstName || currentEditingPatient.partnerLastName) {
                document.getElementById('partnerFirstName').value = currentEditingPatient.partnerFirstName || '';
                document.getElementById('partnerMiddleName').value = currentEditingPatient.partnerMiddleName || '';
                document.getElementById('partnerLastName').value = currentEditingPatient.partnerLastName || '';
            } else if (currentEditingPatient.partnerName) {
                // Old patient - split full name: everything but last word = first, last word = last
                var parts = currentEditingPatient.partnerName.trim().split(/\s+/);
                if (parts.length === 1) {
                    document.getElementById('partnerFirstName').value = parts[0];
                    document.getElementById('partnerLastName').value = '';
                } else {
                    document.getElementById('partnerLastName').value = parts.pop();
                    document.getElementById('partnerFirstName').value = parts.join(' ');
                }
                document.getElementById('partnerMiddleName').value = '';
            } else {
                document.getElementById('partnerFirstName').value = '';
                document.getElementById('partnerMiddleName').value = '';
                document.getElementById('partnerLastName').value = '';
            }
            document.getElementById('partnerAlias').value = currentEditingPatient.partnerAlias || '';
            document.getElementById('patientPhone').value = currentEditingPatient.patientPhone || currentEditingPatient.phone || '';
            document.getElementById('patientEmail').value = currentEditingPatient.patientEmail || currentEditingPatient.email || '';
            document.getElementById('partnerPhone').value = currentEditingPatient.partnerPhone || '';
            document.getElementById('partnerEmail').value = currentEditingPatient.partnerEmail || '';
            document.getElementById('notes').value = currentEditingPatient.notes || '';
            document.getElementById('isSurvivorshipClinic').checked = currentEditingPatient.isSurvivorshipClinic || false;
            document.getElementById('isOTC').checked = currentEditingPatient.isOTC || false;
            document.getElementById('isPriorityList').checked = currentEditingPatient.isPriorityList || false;
            
            document.getElementById('patientModal').classList.add('active');
        }

		var currentCancellingApptPatient = null;
        
        function cancelAppointment(patientID) {
            if (isReadOnly) {
                showErrorModal('Cannot cancel appointments - database is in read-only mode.');
                return;
            }
            
            var patient = null;
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patient = patients[i];
                    break;
                }
            }
            
            if (!patient || !patient.nextAppointment) {
                // Fallback: use browser confirm if patient data not available
                if (confirm('Are you sure you want to cancel this appointment?')) {
                    // Try to find the patient again or handle directly
                    console.error('Patient not found or no appointment:', patientID);
                }
                return;
            }
            
            currentCancellingApptPatient = patient;
            document.getElementById('cancelApptPatientName').textContent = formatNameWithAlias(patient.patientName, patient.patientAlias, patient.patientFirstName, patient.patientMiddleName, patient.patientLastName);
            document.getElementById('cancelApptModal').classList.add('active');
        }
        
        async function confirmCancelAppointment(cancelledBy) {
            if (!currentCancellingApptPatient) return;
            
            // Lock to prevent auto-refresh collision
            isManualOperationInProgress = true;
            startTiming('confirmCancelAppointment');
            
            try {
                var summaryText = cancelledBy === 'doctor' ? 'Cancelled by doctor' : 'Cancelled by patient';
                
                // Determine the correct state to go back to
                var newState = 'WAITING_NEXT_APPT_SCHEDULE';
                
                // Check if this was their first appointment (no completed appointments in history)
                var hasCompletedAppt = false;
                for (var j = 0; j < currentCancellingApptPatient.appointmentHistory.length; j++) {
                    var summary = currentCancellingApptPatient.appointmentHistory[j].summary || '';
                    if (summary.toLowerCase().indexOf('cancelled') === -1) {
                        hasCompletedAppt = true;
                        break;
                    }
                }
                
                if (!hasCompletedAppt) {
                    newState = 'WAITING_FIRST_APPT_SCHEDULE';
                }
                
                // SAVE TO BACKEND: Add cancellation to appointment history
                startTiming('add_appointment_history');
                await eel.add_appointment_history(
                    currentCancellingApptPatient.patientID,
                    currentCancellingApptPatient.nextAppointment,
                    currentCancellingApptPatient.appointmentTime || '',
                    currentCancellingApptPatient.appointmentLocation || '',
                    summaryText
                )();
                endTiming('add_appointment_history');
                
                // SAVE TO BACKEND: Clear appointment
                startTiming('update_next_appointment');
                await eel.update_next_appointment(currentCancellingApptPatient.patientID, null, null, null)();
                endTiming('update_next_appointment');
                
                // SAVE TO BACKEND: Update state (optimized - only loads this one patient!)
                startTiming('updatePatientStateWithSave');
                await updatePatientStateWithSave(currentCancellingApptPatient.patientID, newState, summaryText);
                endTiming('updatePatientStateWithSave');
                
                var cancelledPatientID = currentCancellingApptPatient.patientID;
                currentCancellingApptPatient = null;
                closeModal('cancelApptModal');
                
                startTiming('renderAppointments');
                await renderAppointments();
                endTiming('renderAppointments');
                
                startTiming('renderPatientList');
                renderPatientList();
                endTiming('renderPatientList');
                
                startTiming('updateStatusCounts');
                updateStatusCounts();
                endTiming('updateStatusCounts');
                
                // Refresh patient details if viewing this patient
                if (currentViewingPatientID === cancelledPatientID) {
                    startTiming('viewPatientDetails');
                    viewPatientDetails(currentViewingPatientID);
                    endTiming('viewPatientDetails');
                }
                
                endTiming('confirmCancelAppointment');
            } finally {
                // Always unlock, even if error
                isManualOperationInProgress = false;
            }
            
            endTiming('confirmCancelAppointment');
		}

        // Appointment editing
        function editAppointment(patientID) {
            if (isReadOnly) {
                showErrorModal('Cannot edit appointments - database is in read-only mode.');
                return;
            }
            
            var patient = null;
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patient = patients[i];
                    break;
                }
            }
            if (!patient || !patient.nextAppointment) return;

            currentEditingApptPatient = patient;
            
			// Set the values
            document.getElementById('editApptDate').value = patient.nextAppointment;
            document.getElementById('editApptTime').value = patient.appointmentTime || '';
            document.getElementById('editApptLocation').value = patient.appointmentLocation || '';
			document.getElementById('editApptPatientName').textContent = formatNameWithAlias(patient.patientName, patient.patientAlias, patient.patientFirstName, patient.patientMiddleName, patient.patientLastName);
			
            // Show the modal
            document.getElementById('editApptModal').classList.add('active');
            document.getElementById('dayViewPanel').style.display = 'block';
			updateDayViewPanel(document.getElementById('editApptDate').value);

            // Initialize Flatpickr after a delay to ensure DOM is ready
            setTimeout(function() {
				initializeModalDatePickers();
			}, 150);
        }

        // Validate time format (HH:mm)
        function validateTimeFormat(timeStr) {
            if (!timeStr || timeStr === '') return true; // Empty is OK
            
            var pattern = /^([0-1][0-9]|2[0-3]):([0-5][0-9])$/;
            return pattern.test(timeStr);
        }

		// Check for appointment time conflicts
        function checkAppointmentConflict(date, time, excludePatientID) {
            if (!date || !time) return null; // No conflict if date or time is empty
            
            for (var i = 0; i < patients.length; i++) {
                var p = patients[i];
                // Skip the patient we're editing
                if (p.patientID === excludePatientID) continue;
                
                // Check if same date and time
                if (p.nextAppointment === date && p.appointmentTime === time) {
                    return p; // Return the conflicting patient
                }
            }
            return null; // No conflict
        }

        async function saveAppointmentEdit(event) {
            event.preventDefault();
            
            if (!currentEditingApptPatient) return;

			var timeValue = document.getElementById('editApptTime').value;
            var dateValue = document.getElementById('editApptDate').value;
            
            // Validate time format if provided
            if (timeValue && !validateTimeFormat(timeValue)) {
                showErrorModal('Invalid time format. Please use HH:mm (e.g., 09:30 or 14:45)');
                return;
            }
            
            // Check for appointment conflict
            var conflictPatient = checkAppointmentConflict(dateValue, timeValue, currentEditingApptPatient.patientID);
            if (conflictPatient) {
                var confirmConflict = confirm('Warning: ' + conflictPatient.patientName + ' already has an appointment at this date and time.\n\nDo you want to schedule anyway (e.g., for same-sex couples)?');
                if (!confirmConflict) {
                    return;
                }
            }

			for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === currentEditingApptPatient.patientID) {
					var newDate = document.getElementById('editApptDate').value;
                    var newTime = timeValue;
                    var newLocation = document.getElementById('editApptLocation').value;
                    var oldDate = patients[i].nextAppointment;
                    var oldTime = patients[i].appointmentTime || '';
                    var oldLocation = patients[i].appointmentLocation || '';
                    
					// Only add to history if DATE changed (true reschedule)
                    // Time/location changes on the same date are minor edits - no history entry
                    if (oldDate && oldDate !== newDate) {
                        // SAVE TO BACKEND: Add reschedule to appointment history
                        await eel.add_appointment_history(
                            currentEditingApptPatient.patientID,
                            oldDate,
                            oldTime,
                            oldLocation,
                            'Rescheduled to ' + newDate + (newTime ? ' ' + newTime : '') + (newLocation ? ' (' + newLocation + ')' : '')
                        )();
                    }
                    
                    // SAVE TO BACKEND: Update appointment (already updates local patient!)
                    await scheduleAppointmentWithSave(currentEditingApptPatient.patientID, newDate, newTime, newLocation);
                    
                    break;
                }
            }
			
			var editedPatientID = currentEditingApptPatient.patientID;
            currentEditingApptPatient = null;
            closeModal('editApptModal');
            renderAppointments();
            renderPatientList();
            
            // Refresh patient details if viewing this patient
            if (currentViewingPatientID === editedPatientID) {
                viewPatientDetails(currentViewingPatientID);
            }
			
        }

        // State transitions
        function initiateStateTransition(patientID, nextState) {
            if (isReadOnly) {
                showErrorModal('Cannot modify patients - database is in read-only mode.');
                return;
            }
            
            var patient = null;
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patient = patients[i];
                    break;
                }
            }
            if (!patient) return;

            currentTransitionPatient = { patient: patient, nextState: nextState };
            
            var fields = '';
            var title = '';

			if (nextState === 'WAITING_FIRST_APPT' || nextState === 'WAITING_NEXT_APPT') {
                title = 'Schedule Appointment';
                var defaultLocation = (nextState === 'WAITING_FIRST_APPT') ? 'Vaughan' : 'Virtual';
                fields = '<div class="form-group"><label class="form-label">Appointment Date *</label>' +
                    '<input type="text" class="form-input flatpickr-date" id="transitionDate" required></div>' +
                    '<div class="form-group"><label class="form-label">Appointment Time</label>' +
                    '<input type="text" class="form-input" id="transitionTime" placeholder="HH:mm"></div>' +
                    '<div class="form-group"><label class="form-label">Location</label>' +
                    '<select class="form-input" id="transitionLocation">' +
                    '<option value="Virtual"' + (defaultLocation === 'Virtual' ? ' selected' : '') + '>Virtual</option>' +
                    '<option value="Vaughan"' + (defaultLocation === 'Vaughan' ? ' selected' : '') + '>Vaughan</option>' +
                    '<option value="Downtown"' + (defaultLocation === 'Downtown' ? ' selected' : '') + '>Downtown</option>' +
                    '</select></div>';
				    
					setTimeout(function() {
						document.getElementById('dayViewPanel').style.display = 'block';
						updateDayViewPanel('');
					}, 150);
			} else if (nextState === 'WAITING_APPT_SUMMARY') {
                title = 'Appointment Completed';
                fields = '<div class="form-group"><label class="form-label">Appointment Summary</label>' +
                    '<textarea class="form-input" id="transitionSummary" rows="4"></textarea></div>';
            } else if (nextState === 'WAITING_NEXT_APPT_SCHEDULE') {
                title = 'Ready for Next Appointment';
                fields = '<div class="form-group"><label class="form-label">Additional Notes (Optional)</label>' +
                    '<textarea class="form-input" id="transitionNotes" rows="3"></textarea></div>';
            }

            document.getElementById('transitionTitle').textContent = title;
            document.getElementById('transitionFields').innerHTML = fields;
            document.getElementById('transitionModal').classList.add('active');
            
            // Initialize Flatpickr for the new date/time inputs after a short delay
            setTimeout(function() {
                initializeModalDatePickers();
            }, 100);
        }

        async function completeTransition(event) {
            event.preventDefault();
            
            if (!currentTransitionPatient) return;

            var patient = currentTransitionPatient.patient;
            var nextState = currentTransitionPatient.nextState;
            
			// Validate time format if scheduling an appointment
            if (nextState === 'WAITING_FIRST_APPT' || nextState === 'WAITING_NEXT_APPT') {
                var timeValue = document.getElementById('transitionTime') ? document.getElementById('transitionTime').value : '';
                var dateValue = document.getElementById('transitionDate') ? document.getElementById('transitionDate').value : '';
                
                if (timeValue && !validateTimeFormat(timeValue)) {
                    showErrorModal('Invalid time format. Please use HH:mm (e.g., 09:30 or 14:45)');
                    return;
                }

                // Check for appointment conflict
                var conflictPatient = checkAppointmentConflict(dateValue, timeValue, patient.patientID);
                if (conflictPatient) {
                    var confirmConflict = confirm('Warning: ' + conflictPatient.patientName + ' already has an appointment at this date and time.\n\nDo you want to schedule anyway (e.g., for same-sex couples)?');
                    if (!confirmConflict) {
                        return;
                    }
                }
            }

            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patient.patientID) {
                    // Handle state-specific data
					if (nextState === 'WAITING_FIRST_APPT' || nextState === 'WAITING_NEXT_APPT') {
                        var date = document.getElementById('transitionDate').value;
                        var time = document.getElementById('transitionTime').value;
                        var location = document.getElementById('transitionLocation').value;
                        
                        // SAVE APPOINTMENT TO DATABASE (also updates local array)
                        await scheduleAppointmentWithSave(patient.patientID, date, time, location);
					} else if (nextState === 'WAITING_APPT_SUMMARY') {
                        var summary = document.getElementById('transitionSummary') ? document.getElementById('transitionSummary').value : '';
                        if (patient.nextAppointment) {
                            // Save to backend appointment_history table
                            await eel.add_appointment_history(
                                patient.patientID,
                                patient.nextAppointment,
                                patient.appointmentTime || '',
                                patient.appointmentLocation || '',
                                summary
                            )();
                            
                            // CLEAR appointment in backend (appointment is now complete)
                            await eel.update_next_appointment(patient.patientID, null, null, null)();
                        }
                    } else if (nextState === 'WAITING_NEXT_APPT_SCHEDULE') {
                        var notes = document.getElementById('transitionNotes') ? document.getElementById('transitionNotes').value : '';
                        if (notes) {
                            // Save to backend notes_history table
                            await eel.add_note_history(patient.patientID, notes)();
                        }
                    }

                    // Save state to backend (which also updates local array and reloads this patient!)
                    var notes = null;
                    if (nextState === 'WAITING_NEXT_APPT_SCHEDULE') {
                        notes = document.getElementById('transitionNotes') ? document.getElementById('transitionNotes').value : null;
                    } else if (nextState === 'WAITING_APPT_SUMMARY') {
                        notes = document.getElementById('transitionSummary') ? document.getElementById('transitionSummary').value : null;
                    }
                    await updatePatientStateWithSave(patient.patientID, nextState, notes);
                    
                    break;
                }
            }

            closeModal('transitionModal');
			// Refresh patient details view instead of closing it
			if (currentViewingPatientID) {
				viewPatientDetails(currentViewingPatientID);
			}
            renderPatientList();
            renderAppointments();
            updateStatusCounts();
        }

        // Warn before closing if there are unsaved changes and delete lock file
        
        // ============================================================================
        // PROFESSIONAL MODAL HELPERS (Replace alert/confirm/prompt)
        // ============================================================================
        
        var confirmModalCallback = null;
        var promptModalCallback = null;
        
        function showConfirm(message, title, callback) {
            document.getElementById('confirmModalTitle').textContent = title || 'Confirm Action';
            document.getElementById('confirmModalMessage').textContent = message;
            document.getElementById('confirmModal').style.display = 'flex';
            confirmModalCallback = callback;
        }
        
        function closeConfirmModal(result) {
            document.getElementById('confirmModal').style.display = 'none';
            if (confirmModalCallback) {
                confirmModalCallback(result);
                confirmModalCallback = null;
            }
        }
        
        function showInfo(message, title) {
            document.getElementById('infoModalTitle').textContent = title || 'Success';
            // Replace newlines with <br> for proper display
            document.getElementById('infoModalMessage').innerHTML = message.replace(/\n/g, '<br>');
            document.getElementById('infoModal').style.display = 'flex';
        }
        
        function closeInfoModal() {
            document.getElementById('infoModal').style.display = 'none';
        }
        
        function showPrompt(message, title, defaultValue, callback) {
            document.getElementById('promptModalTitle').textContent = title || 'Enter Value';
            document.getElementById('promptModalMessage').textContent = message;
            document.getElementById('promptModalInput').value = defaultValue || '';
            document.getElementById('promptModal').style.display = 'flex';
            promptModalCallback = callback;
            
            // Focus input
            setTimeout(function() {
                document.getElementById('promptModalInput').focus();
            }, 100);
            
            // Handle Enter key
            document.getElementById('promptModalInput').onkeypress = function(e) {
                if (e.keyCode === 13) {
                    closePromptModal('submit');
                }
            };
        }
        
        function closePromptModal(action) {
            var value = null;
            if (action === 'submit') {
                value = document.getElementById('promptModalInput').value;
            }
            document.getElementById('promptModal').style.display = 'none';
            if (promptModalCallback) {
                promptModalCallback(value);
                promptModalCallback = null;
            }
        }
        
        // ============================================================================
        // USER MANAGEMENT FUNCTIONS
        // ============================================================================
        
        async function openUsersModal() {
            document.getElementById('usersModal').style.display = 'flex';
            await loadUsers();
        }
        
        function closeUsersModal() {
            document.getElementById('usersModal').style.display = 'none';
        }
        
        async function loadUsers() {
            try {
                var result = await eel.get_users()();
                
                if (result.status === 'error') {
                    showErrorModal('Error loading users: ' + result.message);
                    return;
                }
                
                var tbody = document.getElementById('usersTableBody');
                tbody.innerHTML = '';
                
                if (result.users.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 20px;">No users found</td></tr>';
                    return;
                }
                
                result.users.forEach(function(user) {
                    var row = document.createElement('tr');
                    
                    // Username
                    var usernameCell = document.createElement('td');
                    usernameCell.textContent = user.username;
                    row.appendChild(usernameCell);
                    
                    // Last Login
                    var loginCell = document.createElement('td');
                    if (user.lastLogin) {
                        var date = new Date(user.lastLogin * 1000);
                        loginCell.textContent = date.toLocaleDateString('en-US', {timeZone: 'America/Toronto'}) + ' ' + date.toLocaleTimeString('en-US', {timeZone: 'America/Toronto'});
                    } else {
                        loginCell.textContent = 'Never';
                    }
                    row.appendChild(loginCell);
                    
                    // Admin checkbox
                    var adminCell = document.createElement('td');
                    adminCell.style.textAlign = 'center';
                    var adminCheckbox = document.createElement('input');
                    adminCheckbox.type = 'checkbox';
                    adminCheckbox.checked = user.isAdmin;
                    adminCheckbox.disabled = (user.username === 'admin');
                    adminCheckbox.onchange = (function(username, checked) {
                        return function() {
                            toggleAdminStatus(username, checked);
                        };
                    })(user.username, adminCheckbox.checked);
                    adminCell.appendChild(adminCheckbox);
                    row.appendChild(adminCell);
                    
                    // Actions
                    var actionsCell = document.createElement('td');
                    actionsCell.style.textAlign = 'center';
                    
                    // Reset Password button
                    var resetBtn = document.createElement('button');
                    resetBtn.className = 'btn btn-sm';
                    resetBtn.style.cssText = 'background: #17a2b8; color: white; margin-right: 5px; padding: 4px 8px; font-size: 12px;';
                    resetBtn.textContent = 'Reset Password';
                    resetBtn.onclick = (function(username) {
                        return function() {
                            resetUserPassword(username);
                        };
                    })(user.username);
                    actionsCell.appendChild(resetBtn);
                    
                    // Delete button (not for 'admin')
                    if (user.username !== 'admin') {
                        var deleteBtn = document.createElement('button');
                        deleteBtn.className = 'btn btn-sm';
                        deleteBtn.style.cssText = 'background: #dc3545; color: white; padding: 4px 8px; font-size: 12px;';
                        deleteBtn.textContent = 'Delete';
                        deleteBtn.onclick = (function(username) {
                            return function() {
                                deleteUser(username);
                            };
                        })(user.username);
                        actionsCell.appendChild(deleteBtn);
                    }
                    
                    row.appendChild(actionsCell);
                    tbody.appendChild(row);
                });
                
            } catch (error) {
                console.error('Error loading users:', error);
                showErrorModal('Error loading users');
            }
        }
        
        async function addUserFromForm(event) {
            event.preventDefault();
            
            var username = document.getElementById('newUsername').value.trim();
            var password = document.getElementById('newPassword').value;
            var isAdmin = document.getElementById('newIsAdmin').checked;
            
            try {
                var result = await eel.add_user(username, password, isAdmin)();
                
                if (result.status === 'error') {
                    showErrorModal(result.message);
                    return;
                }
                
                showInfo(result.message, 'Success');
                
                // Clear form
                document.getElementById('addUserForm').reset();
                
                // Reload users list
                await loadUsers();
                
            } catch (error) {
                console.error('Error adding user:', error);
                showErrorModal('Error adding user');
            }
        }
        
        async function resetUserPassword(username) {
            showPrompt(
                'Must be at least 8 characters with lowercase, uppercase, and numbers.',
                'Reset Password for ' + username,
                '',
                async function(newPassword) {
                    if (!newPassword) return;
                    
                    try {
                        var result = await eel.update_user_password(username, newPassword)();
                        
                        if (result.status === 'error') {
                            showErrorModal(result.message);
                            return;
                        }
                        
                        showInfo(result.message, 'Success');
                        
                    } catch (error) {
                        console.error('Error resetting password:', error);
                        showErrorModal('Error resetting password');
                    }
                }
            );
        }
        
        async function toggleAdminStatus(username, currentChecked) {
            try {
                var newStatus = !currentChecked;
                var result = await eel.update_user_admin(username, newStatus)();
                
                if (result.status === 'error') {
                    showErrorModal(result.message);
                    // Reload to reset checkbox
                    await loadUsers();
                    return;
                }
                
                showInfo(result.message, 'Success');
                
            } catch (error) {
                console.error('Error updating admin status:', error);
                showErrorModal('Error updating admin status');
                await loadUsers();
            }
        }
        
        async function deleteUser(username) {
            showConfirm(
                'Are you sure you want to delete user "' + username + '"?\n\nThis action cannot be undone.',
                'Confirm Delete',
                async function(confirmed) {
                    if (!confirmed) return;
                    
                    try {
                        var result = await eel.remove_user(username)();
                        
                        if (result.status === 'error') {
                            showErrorModal(result.message);
                            return;
                        }
                        
                        showInfo(result.message, 'Success');
                        await loadUsers();
                        
                    } catch (error) {
                        console.error('Error deleting user:', error);
                        showErrorModal('Error deleting user');
                    }
                }
            );
        }
        
		window.onbeforeunload = function() {
            // Unregister session when closing
            if (currentUser) {
                eel.unregister_session(currentUser)();
            }
            
            // Delete lock file when closing (if we own it)
            if (!isReadOnly && currentUser) {
                deleteLockFile();
            }
        };
        
		document.getElementById('loginUsername').focus();

        // ============================================================================
        // ACTION ITEMS FUNCTIONS
        // ============================================================================
        
        // Load action items from JSON file
        function loadActionItems() {
            // Action items loaded from backend via Eel
            // TODO: Implement if needed
        }
        
        // Save action items to JSON file
        function saveActionItems() {
            // Action items saved via backend
            // Individual operations call backend directly
        }
        
        // ============================================================================
        // PORTAL ACCESS CHECKER
        // ============================================================================
        
        // Open portal modal and check missing access
        async function openPortalModal() {
            document.getElementById('portalModal').classList.add('active');
            await loadPortalUsers(false);  // Use cache by default
        }
        
        async function loadPortalUsers(forceRefresh = false) {
            // Show loading
            document.getElementById('portalContent').innerHTML = 
                '<div style="padding: 40px; text-align: center;">' +
                '<div class="spinner"></div><br>' + 
                (forceRefresh ? 'Refreshing from file...' : 'Checking portal access...') + 
                '</div>';
            
            // Call backend
            try {
                var result = await eel.get_missing_portal_access(forceRefresh)();
                
                // Check if result is valid
                if (!result) {
                    document.getElementById('portalContent').innerHTML = 
                        '<div style="padding: 30px; text-align: center; color: #e74c3c;">' +
                        '<div style="font-size: 48px; margin-bottom: 15px;">⚠️</div>' +
                        '<div style="font-size: 16px; font-weight: 600; margin-bottom: 10px;">Error</div>' +
                        '<div style="font-size: 13px; color: #666; margin-bottom: 20px;">No response from backend. Check if xlrd is installed:<br><code>pip install xlrd --break-system-packages</code></div>' +
                        '</div>';
                    return;
                }
                
                // Check for errors
                if (result.error) {
                    document.getElementById('portalContent').innerHTML = 
                        '<div style="padding: 30px; text-align: center; color: #e74c3c;">' +
                        '<div style="font-size: 48px; margin-bottom: 15px;">⚠️</div>' +
                        '<div style="font-size: 16px; font-weight: 600; margin-bottom: 10px;">Error Reading Portal File</div>' +
                        '<div style="font-size: 13px; color: #666; margin-bottom: 20px;">' + result.error + '</div>' +
                        '<div style="font-size: 12px; color: #999;">Expected file: DB/Patient Portal Users.xls</div>' +
                        '</div>';
                    return;
                }
                
                // Display results
                displayPortalResults(result.missing);
            } catch (error) {
                console.error('Portal error:', error);
                document.getElementById('portalContent').innerHTML = 
                    '<div style="padding: 30px; text-align: center; color: #e74c3c;">' +
                    '<div style="font-size: 48px; margin-bottom: 15px;">⚠️</div>' +
                    '<div style="font-size: 16px; font-weight: 600; margin-bottom: 10px;">Error</div>' +
                    '<div style="font-size: 13px; color: #666; margin-bottom: 20px;">' + error + '</div>' +
                    '<div style="font-size: 12px; color: #999;">Check console for details. Install xlrd: pip install xlrd --break-system-packages</div>' +
                    '</div>';
            }
        }
        
        function displayPortalResults(missing) {
            var html = '';
            
            if (missing.length === 0) {
                html = '<div style="padding: 40px; text-align: center; color: #27ae60; font-size: 14px;">' +
                       '<div style="font-size: 48px; margin-bottom: 15px;">✓</div>' +
                       '<div style="font-weight: 600;">All patients with appointments in the next 3 weeks have portal access!</div>' +
                       '</div>';
            } else {
                html += '<div style="padding: 15px; background: #fff3cd; border: 1px solid #ffc107; margin: 15px; border-radius: 4px; font-size: 13px;">' +
                        '⚠️ <strong>' + missing.length + '</strong> patient(s)/partner(s) missing portal access</div>';
                
                html += '<div style="padding: 0 15px 15px 15px;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #f5f5f5; font-weight: 600;">' +
                        '<th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">ID</th>' +
                        '<th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Name</th>' +
                        '<th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Type</th>' +
                        '<th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Appointment</th>' +
                        '</tr></thead><tbody>';
                
                for (var i = 0; i < missing.length; i++) {
                    var item = missing[i];
                    // Simple date format: YYYY-MM-DD
                    var dateStr = item.appointmentDate;
                    
                    html += '<tr style="border-bottom: 1px solid #eee;">' +
                            '<td style="padding: 8px;">' + item.id + '</td>' +
                            '<td style="padding: 8px;">' + item.name + '</td>' +
                            '<td style="padding: 8px;">' + item.type + '</td>' +
                            '<td style="padding: 8px;">' + dateStr + '</td>' +
                            '</tr>';
                }
                
                html += '</tbody></table></div>';
            }
            
            document.getElementById('portalContent').innerHTML = html;
        }
        
        // ============================================================================
        // ACTION ITEMS MODAL
        // ============================================================================
        
        // Open action items modal
        function openActionItemsModal() {
            // Always reload from file when opening
            loadActionItems();
            
            // Apply saved active tab
            applyActiveTab(actionItems.activeTab || 'all');
            
            renderActionItems();
            
            // Reset dropdowns to placeholder
            document.getElementById('newActionItemType').selectedIndex = 0;
            document.getElementById('newActionItemPriority').selectedIndex = 0;
            document.getElementById('newActionItemText').value = '';
            
            // Hide add bar in read-only mode
            var addBar = document.getElementById('actionItemsAddBar');
            if (addBar) {
                addBar.style.display = isReadOnly ? 'none' : 'block';
            }
            
            document.getElementById('actionItemsModal').classList.add('active');
        }
        
        // Switch action items tab
        function switchActionTab(tab) {
            var previousTab = actionItems.activeTab;
            
            // Reload from file to get latest changes
            loadActionItems();
            
            // Apply the new tab
            actionItems.activeTab = tab;
            applyActiveTab(tab);
            renderActionItems();
            
            // Only save if tab actually changed
            if (tab !== previousTab && !isReadOnly) {
                saveActionItems();
            }
        }
        
        // Apply active tab display
        function applyActiveTab(tab) {
            // Update tab buttons
            var tabButtons = document.getElementById('actionItemTabs');
            if (tabButtons) {
                var buttons = tabButtons.getElementsByTagName('button');
                for (var j = 0; j < buttons.length; j++) {
                    buttons[j].className = 'action-tab';
                    var btnText = buttons[j].innerText.toLowerCase().replace('-', '');
                    if (btnText === tab || (tab === 'email' && btnText === 'email')) {
                        buttons[j].className = 'action-tab active';
                    }
                }
            }
            
            // Show/hide boxes and resize to fill space
            var boxes = ['Appointment', 'General', 'Phone', 'Email'];
            var visibleCount = (tab === 'all') ? 4 : 1;
            
            for (var k = 0; k < boxes.length; k++) {
                var container = document.getElementById('box' + boxes[k] + 'Container');
                var boxInner = container ? container.getElementsByTagName('div')[0] : null;
                var content = document.getElementById('actionItems' + boxes[k]);
                
                if (container) {
                    if (tab === 'all') {
                        // Show all 4 boxes in 2x2 grid
                        container.style.display = 'block';
                        container.style.width = '50%';
                        if (boxInner) boxInner.style.height = '245px';
                        if (content) content.style.height = '195px';
                    } else if (tab === boxes[k].toLowerCase()) {
                        // Show single box, full width and height
                        container.style.display = 'block';
                        container.style.width = '100%';
                        if (boxInner) boxInner.style.height = '510px';
                        if (content) content.style.height = '460px';
                    } else {
                        container.style.display = 'none';
                    }
                }
            }
        }
        
        // Close action items modal (with save)
        function closeActionItemsModal() {
            // Save before closing (if not read-only)
            if (!isReadOnly) {
                saveActionItems();
            }
            document.getElementById('actionItemsModal').classList.remove('active');
        }
        
        // Add new action item
        function addActionItem() {
            if (isReadOnly) {
                showErrorModal('Cannot add action items - database is in read-only mode.');
                return;
            }
            
            var text = document.getElementById('newActionItemText').value.trim();
            var type = document.getElementById('newActionItemType').value;
            var priority = document.getElementById('newActionItemPriority').value;
            
            if (!text) {
                showErrorModal('Please enter an action item description.');
                return;
            }
            
            if (!type) {
                showErrorModal('Please select a type.');
                return;
            }
            
            if (!priority) {
                showErrorModal('Please select a priority.');
                return;
            }
            
            var newItem = {
                id: Date.now().toString(),
                text: text,
                priority: priority,
                addedAt: new Date().toISOString(),
                done: false,
                doneAt: null
            };
            
            actionItems[type].push(newItem);
            saveActionItems(); // Save immediately after adding
            renderActionItems();
            
            // Clear input and reset dropdowns
            document.getElementById('newActionItemText').value = '';
            document.getElementById('newActionItemType').selectedIndex = 0;
            document.getElementById('newActionItemPriority').selectedIndex = 0;
        }
        
        // Toggle action item done status
        function toggleActionItemDone(type, itemId) {
            if (isReadOnly) {
                showErrorModal('Cannot modify action items - database is in read-only mode.');
                return;
            }
            
            for (var i = 0; i < actionItems[type].length; i++) {
                if (actionItems[type][i].id === itemId) {
                    var item = actionItems[type][i];
                    item.done = !item.done;
                    item.doneAt = item.done ? new Date().toISOString() : null;
                    break;
                }
            }
            
            saveActionItems(); // Save immediately after change
            renderActionItems();
        }
        
        // Get priority color
        function getPriorityColor(priority) {
            switch (priority) {
                case 'high': return '#e74c3c';
                case 'medium': return '#e67e22';
                case 'low': return '#000000';
                default: return '#000000';
            }
        }
        
        // Format date/time for tooltip
        function formatDateTime(isoString) {
            var d = new Date(isoString);
            return d.toLocaleDateString('en-US', {timeZone: 'America/Toronto'}) + ' ' + d.toLocaleTimeString('en-US', {timeZone: 'America/Toronto'});
        }
        
        // Render action items for all types
        function renderActionItems() {
            renderActionItemsForType('appointment', 'actionItemsAppointment');
            renderActionItemsForType('general', 'actionItemsGeneral');
            renderActionItemsForType('phone', 'actionItemsPhone');
            renderActionItemsForType('email', 'actionItemsEmail');
        }
        
        // Render action items for a specific type
        function renderActionItemsForType(type, containerId) {
            var container = document.getElementById(containerId);
            if (!container) return;
            
            var items = actionItems[type] || [];
            var now = new Date();
            var oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
            
            // Separate open and done items
            var openItems = [];
            var doneItems = [];
            
            for (var i = 0; i < items.length; i++) {
                var item = items[i];
                if (!item.done) {
                    openItems.push(item);
                } else {
                    // Only include done items from the last week
                    var doneDate = new Date(item.doneAt);
                    if (doneDate >= oneWeekAgo) {
                        doneItems.push(item);
                    }
                }
            }
            
            // Sort open items by priority (high -> medium -> low) then by date added
            var priorityOrder = { high: 0, medium: 1, low: 2 };
            openItems.sort(function(a, b) {
                var priorityDiff = priorityOrder[a.priority] - priorityOrder[b.priority];
                if (priorityDiff !== 0) return priorityDiff;
                return new Date(a.addedAt) - new Date(b.addedAt);
            });
            
            // Sort done items by done date (newest first) and limit to 10
            doneItems.sort(function(a, b) {
                return new Date(b.doneAt) - new Date(a.doneAt);
            });
            doneItems = doneItems.slice(0, 10);
            
            // Build HTML
            var html = '';
            
            if (openItems.length === 0 && doneItems.length === 0) {
                html = '<div class="empty-state">No action items</div>';
            } else {
                // Render open items
                for (var j = 0; j < openItems.length; j++) {
                    var openItem = openItems[j];
                    var color = getPriorityColor(openItem.priority);
                    var addedDateTime = formatDateTime(openItem.addedAt);
                    
                    html += '<div style="display: flex; align-items: center; padding: 8px 0; border-bottom: 1px solid #eee;" title="Added: ' + addedDateTime + '">';
                    html += '<input type="checkbox" onchange="toggleActionItemDone(\'' + type + '\', \'' + openItem.id + '\')" style="margin-right: 10px; cursor: pointer;"' + (isReadOnly ? ' disabled' : '') + '>';
                    html += '<span style="color: ' + color + '; flex: 1;">' + escapeHtml(openItem.text) + '</span>';
                    html += '</div>';
                }
                
                // Render done items (with strikethrough)
                for (var k = 0; k < doneItems.length; k++) {
                    var doneItem = doneItems[k];
                    var doneDateTime = formatDateTime(doneItem.doneAt);
                    
                    html += '<div style="display: flex; align-items: center; padding: 8px 0; border-bottom: 1px solid #eee; opacity: 0.6;" title="Done: ' + doneDateTime + '">';
                    html += '<input type="checkbox" checked onchange="toggleActionItemDone(\'' + type + '\', \'' + doneItem.id + '\')" style="margin-right: 10px; cursor: pointer;"' + (isReadOnly ? ' disabled' : '') + '>';
                    html += '<span style="text-decoration: line-through; color: #888; flex: 1;">' + escapeHtml(doneItem.text) + '</span>';
                    html += '</div>';
                }
            }
            
            container.innerHTML = html;
        }
        
        // Escape HTML special characters
        function escapeHtml(text) {
            var div = document.createElement('div');
            div.appendChild(document.createTextNode(text));
            return div.innerHTML;
        }

        // ============================================================================
        // EMAIL GENERATOR FUNCTIONS
        // ============================================================================
        
        var currentEmailPatient = null;
        var emailTemplates = null;
        var currentViewingPatientID = null; // Store patient ID when viewing details
        
        // Load email templates from JSON file
        // Load email templates from JSON file
        async function loadEmailTemplates() {
            // Load email templates from backend
            try {
                emailTemplates = await eel.get_email_templates()();
                if (!emailTemplates) {
                    console.error('Failed to load email templates');
                    return false;
                }
                return true;
            } catch (error) {
                console.error('Error loading email templates:', error);
                return false;
            }
        }
        
        // Open email generator modal
        async function openEmailGenerator() {
            // Get current patient from stored ID
            currentEmailPatient = null;
            
            if (!currentViewingPatientID) {
                showErrorModal('No patient selected');
                return;
            }
            
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === currentViewingPatientID) {
                    currentEmailPatient = patients[i];
                    break;
                }
            }
            
            if (!currentEmailPatient) {
                showErrorModal('Patient not found');
                return;
            }
            
            // Load templates if not already loaded
            if (!emailTemplates) {
                var loaded = await loadEmailTemplates();
                if (!loaded) {
                    showErrorModal('Failed to load email templates');
                    return;
                }
            }
            
            // Populate patient/partner names
			document.getElementById('emailPatientName').textContent = formatNameWithAlias(currentEmailPatient.patientName, currentEmailPatient.patientAlias, currentEmailPatient.patientFirstName, currentEmailPatient.patientMiddleName, currentEmailPatient.patientLastName) || '-';
            document.getElementById('emailPartnerName').textContent = formatNameWithAlias(currentEmailPatient.partnerName, currentEmailPatient.partnerAlias, currentEmailPatient.partnerFirstName, currentEmailPatient.partnerMiddleName, currentEmailPatient.partnerLastName) || '-';            
			
            // Handle partner checkbox
            var partnerRow = document.getElementById('emailPartnerRow');
            var partnerCheckbox = document.getElementById('emailToPartner');
            if (!currentEmailPatient.partnerName || currentEmailPatient.partnerName.trim() === '') {
                partnerRow.style.opacity = '0.5';
                partnerCheckbox.checked = false;
                partnerCheckbox.disabled = true;
            } else {
                partnerRow.style.opacity = '1';
                partnerCheckbox.checked = true;
                partnerCheckbox.disabled = false;
            }
            
            // Reset form
            document.getElementById('emailType').value = '';
            document.getElementById('emailOptionsContainer').innerHTML = '';
            document.getElementById('previewTo').textContent = '-';
            document.getElementById('previewSubject').textContent = '-';
            document.getElementById('previewBody').innerHTML = '<p style="color: #999;">Select an email type to see preview</p>';
            
            // Show modal
            document.getElementById('emailGeneratorModal').classList.add('active');
        }
        
        // Handle email type change
        function onEmailTypeChange() {
            var emailType = document.getElementById('emailType').value;
            var container = document.getElementById('emailOptionsContainer');
            
            if (!emailType) {
                container.innerHTML = '';
                document.getElementById('previewBody').innerHTML = '<p style="color: #999;">Select an email type to see preview</p>';
                return;
            }
            
            var html = '';
            
            // Common appointment options for most email types
            if (emailType === 'welcome' || emailType === 'firstAppointment' || emailType === 'followUp' || emailType === 'abortion' || emailType === 'reminder' ||
                emailType === 'nextAppointment' || emailType === 'reschedulingPatient' || emailType === 'reschedulingPhysician'  || emailType === 'noShow') {
                
				// Cycle plan included checkbox (only for followUp)
				if (emailType === 'followUp') {
					html += '<div class="form-group" id="cycleOrdersGroup">';
					html += '	<label class="form-label">';
					html += '		<input type="checkbox" id="emailCycleOrders" onchange="toggleCycleOrdersType(); updateEmailPreview()"> Cycle orders included';
					html += '	</label>';
					html += '	<select id="emailCycleOrdersType" class="form-input" style="margin-top: 5px; display: none;" onchange="updateEmailPreview()">';
					html += '		<option value="">-- Select cycle type --</option>';
					html += '		<option value="IUI">IUI</option>';
					html += '		<option value="IVF">IVF</option>';
					html += '		<option value="ovulation">Ovulation Induction</option>';
					html += '		<option value="embryo">Embryo Transfer</option>';
					html += '		<option value="generic">Other/Generic</option>';
					html += '	</select>';
					html += '</div>';		
				}

                // Appointment included checkbox (only for welcome and followUp)
                if (emailType === 'welcome' || emailType === 'followUp' || emailType === 'abortion') {
                    html += '<div style="margin-bottom: 15px;">';
                    html += '<label style="display: block;">';
                    html += '<input type="checkbox" id="emailApptIncluded" checked onchange="toggleApptOptions(); updateEmailPreview();">';
                    html += ' Appointment details included';
                    html += '</label>';
                    html += '</div>';
                }
                
                // Reschedule reason FIRST (for physician rescheduling)
                if (emailType === 'reschedulingPhysician') {
                    html += '<div style="margin-bottom: 12px;">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Reschedule Reason:</label>';
                    html += '<select id="emailRescheduleReason" class="form-input" onchange="toggleTestsIncompleteOptions(); updateEmailPreview();" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '<option value="midTreatment">Mid-treatment - rescheduled to after treatment</option>';
                    html += '<option value="physicianUnavailable">Physician no longer available</option>';
                    html += '<option value="testsIncomplete">Follow-up tests not completed</option>';
                    html += '</select>';
                    html += '</div>';
                    
                    // Tests incomplete options
                    html += '<div id="testsIncompleteOptions" style="display: none; margin-bottom: 12px; padding: 10px; background: #fff8e1; border-radius: 4px;">';
                    html += '<label style="display: block; margin-bottom: 8px;">';
                    html += '<input type="checkbox" id="emailAskIfCanComplete" onchange="updateEmailPreview()">';
                    html += ' Ask if patient can complete tests before the meeting';
                    html += '</label>';
                    html += '<div id="askIfCanCompleteText" style="display: none; font-size: 11px; color: #666; margin-left: 20px;">This will add a question asking if they can complete the tests in time, or if they prefer to reschedule.</div>';
                    html += '</div>';
                }

				// No Show type selection
				if (emailType === 'noShow') {
					html += '<div style="margin-bottom: 12px;">';
					html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Type:</label>';
					html += '<select id="emailNoShowType" class="form-input" onchange="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
					html += '<option value="noShow">No Show</option>';
					html += '<option value="lateCancellation">Late Cancellation</option>';
					html += '</select>';
					html += '</div>';
				}
                
				// Appointment options container
                html += '<div id="apptOptionsContainer">';
                
                // Clinic type (not needed for noShow and reminder)
                if (emailType !== 'noShow' && emailType !== 'reminder') {
                    html += '<div style="margin-bottom: 12px;">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Clinic Type:</label>';
                    html += '<select id="emailClinicType" class="form-input" onchange="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '<option value="Fertility">Fertility</option>';
                    html += '<option value="Fertility Preservation">Fertility Preservation</option>';
                    html += '<option value="Fertility - Cancer Survivorship">Fertility - Cancer Survivorship</option>';
                    html += '</select>';
                    html += '</div>';
                }
                
                // Appointment type (not needed for noShow and reminder)
                if (emailType !== 'noShow' && emailType !== 'reminder') {
                    // Pre-fill appointment type and location from patient's appointmentLocation
                    var patientApptLocation = currentEmailPatient && currentEmailPatient.appointmentLocation ? currentEmailPatient.appointmentLocation : '';
                    var prefilledApptType = '';
                    var prefilledLocation = '';
                    var showLocationDropdown = 'none';
                    
                    if (patientApptLocation) {
                        var loc = patientApptLocation.toLowerCase();
                        if (loc === 'virtual') {
                            prefilledApptType = 'Virtual (OTN)';
                            prefilledLocation = '';
                            showLocationDropdown = 'none';
                        } else if (loc === 'vaughan') {
                            prefilledApptType = 'In Person';
                            prefilledLocation = 'vaughan';
                            showLocationDropdown = 'block';
                        } else if (loc === 'downtown') {
                            prefilledApptType = 'In Person';
                            prefilledLocation = 'downtown';
                            showLocationDropdown = 'block';
                        }
                    } else {
                        // No appointment location set - use defaults
                        if (emailType === 'welcome' || emailType === 'firstAppointment') {
                            prefilledApptType = 'In Person';
                            prefilledLocation = 'vaughan';
                            showLocationDropdown = 'block';
                        } else {
                            prefilledApptType = 'Virtual (OTN)';
                            prefilledLocation = '';
                            showLocationDropdown = 'none';
                        }
                    }
                    
                    html += '<div style="margin-bottom: 12px;">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Appointment Type:</label>';
                    html += '<select id="emailApptType" class="form-input" onchange="toggleLocationOptions(); updateEmailPreview();" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '<option value="Virtual (OTN)"' + (prefilledApptType === 'Virtual (OTN)' ? ' selected' : '') + '>Virtual (OTN)</option>';
                    html += '<option value="In Person"' + (prefilledApptType === 'In Person' ? ' selected' : '') + '>In Person</option>';
                    html += '</select>';
                    html += '</div>';
                    
                    // Location (for in-person)
                    html += '<div id="locationContainer" style="margin-bottom: 12px; display: ' + showLocationDropdown + ';">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Location:</label>';
                    html += '<select id="emailLocation" class="form-input" onchange="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '<option value="vaughan"' + (prefilledLocation === 'vaughan' ? ' selected' : '') + '>Vaughan</option>';
                    html += '<option value="downtown"' + (prefilledLocation === 'downtown' ? ' selected' : '') + '>Downtown Toronto</option>';
                    html += '</select>';
                    html += '</div>';
                }
                
                // Pre-populate date/time from patient's next appointment
                var prefilledDate = '';
                var prefilledTime = '';
                if (currentEmailPatient && currentEmailPatient.nextAppointment) {
                    prefilledDate = currentEmailPatient.nextAppointment;
                }
                if (currentEmailPatient && currentEmailPatient.appointmentTime) {
                    prefilledTime = currentEmailPatient.appointmentTime;
                }
                
                // Date and Time (not needed for noShow)
                if (emailType !== 'noShow') {
                    html += '<div style="margin-bottom: 12px;">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Date:</label>';
                    html += '<input type="text" id="emailApptDate" class="form-input" placeholder="Select date" value="' + prefilledDate + '" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '</div>';
                    
                    html += '<div style="margin-bottom: 12px;">';
                    html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Time:</label>';
                    html += '<input type="text" id="emailApptTime" class="form-input" placeholder="e.g., 9:00 AM" value="' + prefilledTime + '" oninput="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
                    html += '</div>';
                }
                
                html += '</div>'; // end apptOptionsContainer
			}
            
            // Portal access specific options
            if (emailType === 'portalAccess') {
                html += '<div style="margin-bottom: 12px;">';
                html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Temporary Password:</label>';
                html += '<input type="text" id="emailPortalPassword" class="form-input" value="Welcome123" oninput="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
                html += '</div>';
            }
            
            // Cancellation email options
            if (emailType === 'cancellation') {
                html += '<div style="margin-bottom: 12px;">';
                html += '<label style="font-weight: 500; display: block; margin-bottom: 5px; font-size: 12px;">Cancellation Reason:</label>';
                html += '<select id="emailCancellationReason" class="form-input" onchange="updateEmailPreview()" style="width: 100%; padding: 6px; font-size: 12px;">';
                html += '<option value="pregnant">Patient got pregnant</option>';
                html += '<option value="patientRequest">Patient request</option>';
                html += '<option value="noReach">Unable to reach</option>';
                html += '<option value="other">Other</option>';
                html += '</select>';
                html += '</div>';
            }
            
            // Tests section (for followUp and nextAppointment)
            if (emailType === 'followUp' || emailType === 'nextAppointment') {
                html += '<div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #e0e0e0;">';
                html += '<label style="font-weight: 600; display: block; margin-bottom: 8px;">Tests Required:</label>';
                
                // Patient tests
                html += '<div style="margin-bottom: 10px; padding: 8px; background: #f5f5f5; border-radius: 4px;">';
                html += '<label style="display: block; margin-bottom: 5px;">';
                html += '<input type="checkbox" id="emailTestsPatient" onchange="togglePatientTests(); updateEmailPreview();">';
                html += ' <strong>Patient Tests</strong>';
                html += '</label>';
                html += '<div id="patientTestsContainer" style="display: none; margin-left: 20px; margin-top: 5px;">';
                html += '<div style="margin-bottom: 5px; font-size: 11px; font-weight: 600;">Blood Work:</div>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientViral" onchange="updateEmailPreview()"> Viral Serology</label>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientAMH" onchange="updateEmailPreview()"> AMH</label>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientOther" onchange="updateEmailPreview()"> Other (RPL, PCOS panels, other)</label>';
                html += '<div style="margin: 8px 0 5px 0; font-size: 11px; font-weight: 600;">Imaging:</div>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientUltrasound" onchange="updateEmailPreview()"> Abdominal/Transvaginal Ultrasound</label>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientSonoMSF" onchange="updateEmailPreview()"> Sonohysterogram at MSF</label>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientSonoTNI" onchange="updateEmailPreview()"> Sonohysterogram at TNI</label>';
				html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientOfficeHysteroscopy" onchange="updateEmailPreview()"> Office Hysteroscopy</label>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientOperativeHysteroscopy" onchange="updateEmailPreview()"> Operative Hysteroscopy</label>';                html += '<div style="margin: 8px 0 5px 0; font-size: 11px; font-weight: 600;">Other:</div>';
                html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPatientSperm" onchange="updateEmailPreview()"> Semen Analysis</label>';
                html += '</div>';
                html += '</div>';
                
                // Partner tests (if partner exists)
                if (currentEmailPatient.partnerName && currentEmailPatient.partnerName.trim() !== '') {
                    html += '<div style="margin-bottom: 10px; padding: 8px; background: #f5f5f5; border-radius: 4px;">';
                    html += '<label style="display: block; margin-bottom: 5px;">';
                    html += '<input type="checkbox" id="emailTestsPartner" onchange="togglePartnerTests(); updateEmailPreview();">';
                    html += ' <strong>Partner Tests</strong>';
                    html += '</label>';
                    html += '<div id="partnerTestsContainer" style="display: none; margin-left: 20px; margin-top: 5px;">';
                    html += '<div style="margin-bottom: 5px; font-size: 11px; font-weight: 600;">Blood Work:</div>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerViral" onchange="updateEmailPreview()"> Viral Serology</label>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerAMH" onchange="updateEmailPreview()"> AMH</label>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerOther" onchange="updateEmailPreview()"> Other (RPL, PCOS panels, other)</label>';
                    html += '<div style="margin: 8px 0 5px 0; font-size: 11px; font-weight: 600;">Imaging:</div>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerUltrasound" onchange="updateEmailPreview()"> Abdominal/Transvaginal Ultrasound</label>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerSonoMSF" onchange="updateEmailPreview()"> Sonohysterogram at MSF</label>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerSonoTNI" onchange="updateEmailPreview()"> Sonohysterogram at TNI</label>';
					html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerOfficeHysteroscopy" onchange="updateEmailPreview()"> Office Hysteroscopy</label>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerOperativeHysteroscopy" onchange="updateEmailPreview()"> Operative Hysteroscopy</label>';
                    html += '<div style="margin: 8px 0 5px 0; font-size: 11px; font-weight: 600;">Other:</div>';
                    html += '<label style="display: block; margin: 3px 0 3px 10px; font-size: 11px;"><input type="checkbox" id="testPartnerSperm" onchange="updateEmailPreview()"> Semen Analysis</label>';
                    html += '</div>';
                    html += '</div>';
                }
                
                html += '</div>';
                
                // Additional options
                html += '<div style="margin-top: 10px;">';
                html += '<label style="display: block; margin: 5px 0; font-size: 12px;"><input type="checkbox" id="emailHandouts" onchange="updateEmailPreview()"> Handouts uploaded to portal</label>';
                html += '<label style="display: block; margin: 5px 0; font-size: 12px;"><input type="checkbox" id="emailReferral" onchange="updateEmailPreview()"> Referral included</label>';
                html += '<label style="display: block; margin: 5px 0; font-size: 12px;"><input type="checkbox" id="emailOFP" onchange="updateEmailPreview()"> OFP Funding information</label>';
                html += '</div>';
            }
            
            container.innerHTML = html;
            
            // Initialize date picker
            if (document.getElementById('emailApptDate')) {
                if (typeof flatpickr !== 'undefined') {
                    flatpickr('#emailApptDate', {
                        dateFormat: 'Y-m-d',
                        defaultDate: prefilledDate || null,
                        onChange: function() { updateEmailPreview(); }
                    });
                }
            }
            
            updateEmailPreview();
        }

		function toggleCycleOrdersType() {
            var checkbox = document.getElementById('emailCycleOrders');
            var select = document.getElementById('emailCycleOrdersType');
            if (checkbox && select) {
                select.style.display = checkbox.checked ? 'block' : 'none';
            }
        }

        // Toggle tests incomplete options
        function toggleTestsIncompleteOptions() {
            var reasonSelect = document.getElementById('emailRescheduleReason');
            var optionsDiv = document.getElementById('testsIncompleteOptions');
            if (optionsDiv && reasonSelect) {
                optionsDiv.style.display = reasonSelect.value === 'testsIncomplete' ? 'block' : 'none';
            }
            updateEmailPreview();
        }
        
        // Toggle appointment options visibility
        function toggleApptOptions() {
            var checkbox = document.getElementById('emailApptIncluded');
            var container = document.getElementById('apptOptionsContainer');
            if (container) {
                container.style.display = checkbox && checkbox.checked ? 'block' : 'none';
            }
        }
        
        // Toggle location options based on appointment type
        function toggleLocationOptions() {
            var apptType = document.getElementById('emailApptType');
            var locationContainer = document.getElementById('locationContainer');
            if (locationContainer && apptType) {
                locationContainer.style.display = apptType.value === 'In Person' ? 'block' : 'none';
            }
        }
        
        // Toggle patient tests visibility
        function togglePatientTests() {
            var checkbox = document.getElementById('emailTestsPatient');
            var container = document.getElementById('patientTestsContainer');
            if (container) {
                container.style.display = checkbox && checkbox.checked ? 'block' : 'none';
            }
        }
        
        // Toggle partner tests visibility
        function togglePartnerTests() {
            var checkbox = document.getElementById('emailTestsPartner');
            var container = document.getElementById('partnerTestsContainer');
            if (container) {
                container.style.display = checkbox && checkbox.checked ? 'block' : 'none';
            }
        }
        
        // Update email preview
        function updateEmailPreview() {
            var emailType = document.getElementById('emailType').value;
            if (!emailType || !emailTemplates || !currentEmailPatient) {
                return;
            }
            
            var template = emailTemplates.templates[emailType];
            if (!template) {
                return;
            }
            
            // Build recipient list
            var toList = [];
            var toPatient = document.getElementById('emailToPatient').checked;
            var toPartner = document.getElementById('emailToPartner').checked;
            
			if (toPatient && (currentEmailPatient.patientEmail || currentEmailPatient.email)) {
				toList.push(currentEmailPatient.patientEmail || currentEmailPatient.email);
			}
            if (toPartner && currentEmailPatient.partnerEmail) {
                toList.push(currentEmailPatient.partnerEmail);
            }
            
            document.getElementById('previewTo').textContent = toList.length > 0 ? toList.join('; ') : '(no email addresses)';
            
            // Get first names
            var patientFirstName = getFirstName(currentEmailPatient.patientName, currentEmailPatient.patientAlias, currentEmailPatient.patientFirstName);
            var partnerFirstName = getFirstName(currentEmailPatient.partnerName, currentEmailPatient.partnerAlias, currentEmailPatient.partnerFirstName);
            
            // Build greeting
            var greeting = '';
            if (toPatient && toPartner && currentEmailPatient.partnerName) {
                greeting = patientFirstName + ' and ' + partnerFirstName;
            } else if (toPatient) {
                greeting = patientFirstName;
            } else if (toPartner) {
                greeting = partnerFirstName;
            }
            
            // Partner first name for subject (with " and " prefix if exists)
            var partnerFirstNameForSubject = '';
            if (toPartner && currentEmailPatient.partnerName) {
                partnerFirstNameForSubject = ' and ' + partnerFirstName;
            }
            
			if (!toPatient && toPartner) {
				patientFirstName = getFirstName(currentEmailPatient.partnerName, currentEmailPatient.partnerAlias, currentEmailPatient.partnerFirstName);
				partnerFirstNameForSubject = '';
			}			
			
            // Get form values
            var clinicType = document.getElementById('emailClinicType') ? document.getElementById('emailClinicType').value : '';
            var apptType = document.getElementById('emailApptType') ? document.getElementById('emailApptType').value : '';
            var location = document.getElementById('emailLocation') ? document.getElementById('emailLocation').value : '';
            var apptDate = document.getElementById('emailApptDate') ? document.getElementById('emailApptDate').value : '';
            var apptTime = document.getElementById('emailApptTime') ? document.getElementById('emailApptTime').value : '';
            var portalPassword = document.getElementById('emailPortalPassword') ? document.getElementById('emailPortalPassword').value : 'Welcome123';
            
            // Format date
            var appointmentDay = '';
            var appointmentMonth = '';
            var formattedDate = '';
            if (apptDate) {
                var dateParts = apptDate.split('-');
                var dateObj = new Date(parseInt(dateParts[0]), parseInt(dateParts[1]) - 1, parseInt(dateParts[2]));
                var days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
                var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
                appointmentDay = days[dateObj.getDay()];
                appointmentMonth = months[dateObj.getMonth()] + ' ' + dateObj.getFullYear();
                formattedDate = months[dateObj.getMonth()] + ' ' + dateObj.getDate() + ', ' + dateObj.getFullYear();
            }

			// Handle both partners block - only show if sending to both patient and partner
			var bothPartnersBlock = '';
			if (toPatient && toPartner && apptType === 'In Person') {
				bothPartnersBlock = emailTemplates.commonBlocks.bothPartnersRequired || '';
			}

            
            // Location info - now first in the details block
            var locationInfo = '';
            if (apptType === 'Virtual (OTN)') {
                locationInfo = '';
            } else if (apptType === 'In Person' && location) {
                locationInfo = ' - ' + emailTemplates.locations[location].name + ' (' +  emailTemplates.locations[location].address + ')';
            }
            
			// Build appointment details block
			var appointmentDetails = '';
			if (emailType === 'reminder') {
				// For reminders, use patient's saved location if available
				var patientLocation = currentEmailPatient.appointmentLocation || '';
				if (patientLocation) {
					appointmentDetails = emailTemplates.commonBlocks.appointmentDetails || '';
					apptType = (patientLocation === 'Virtual') ? 'Virtual (OTN)' : 'In Person';
					if (patientLocation === 'Virtual') {
						locationInfo = '';
					} else {
						var locKey = patientLocation.toLowerCase();
						locationInfo = emailTemplates.locations[locKey] ? ' - ' + emailTemplates.locations[locKey].name + ' (' + emailTemplates.locations[locKey].address + ')' : '';
					}
				} else {
					appointmentDetails = emailTemplates.commonBlocks.appointmentDetailsNoLocation || '';
				}
			} else {
				appointmentDetails = emailTemplates.commonBlocks.appointmentDetails || '';
			}

			// Build combined day and date (only show comma if both exist)
			var appointmentDayDate = '';
			if (appointmentDay && formattedDate) {
				appointmentDayDate = appointmentDay + ', ' + formattedDate;
			} else if (formattedDate) {
				appointmentDayDate = formattedDate;
			} else if (appointmentDay) {
				appointmentDayDate = appointmentDay;
			}
			
            appointmentDetails = appointmentDetails.replace(/\{\{appointmentType\}\}/g, apptType);
            appointmentDetails = appointmentDetails.replace(/\{\{locationInfo\}\}/g, locationInfo);
			appointmentDetails = appointmentDetails.replace(/\{\{appointmentDayDate\}\}/g, appointmentDayDate);
            appointmentDetails = appointmentDetails.replace(/\{\{appointmentTime\}\}/g, apptTime);
            
            // Reschedule reason
            var rescheduleReason = '';
            if (document.getElementById('emailRescheduleReason')) {
                var reasonKey = document.getElementById('emailRescheduleReason').value;
                if (emailTemplates.rescheduleReasons && emailTemplates.rescheduleReasons[reasonKey]) {
                    rescheduleReason = emailTemplates.rescheduleReasons[reasonKey];
                }
            }
            
            // Tests incomplete block (for rescheduling) - from JSON
            var testsIncompleteBlock = '';
            if (document.getElementById('emailAskIfCanComplete') && document.getElementById('emailAskIfCanComplete').checked) {
                testsIncompleteBlock = emailTemplates.commonBlocks.testsIncompleteBlock || '';
            }
            
            // Cancellation reason
            var cancellationReason = '';
            if (document.getElementById('emailCancellationReason')) {
                var cancReasonKey = document.getElementById('emailCancellationReason').value;
                if (emailTemplates.cancellationReasons && emailTemplates.cancellationReasons[cancReasonKey]) {
                    cancellationReason = emailTemplates.cancellationReasons[cancReasonKey];
                }
            }
            
            // Determine subject - handle welcome email special case
            var subject = template.subject;
            if (emailType === 'welcome' || emailType === 'abortion') {
                var apptIncluded = document.getElementById('emailApptIncluded');
                if (apptIncluded && !apptIncluded.checked) {
                    subject = template.subjectNoAppt;
                } else {
                    subject = template.subjectWithAppt;
                }
            }
            
            // Replace subject placeholders
            subject = subject.replace(/\{\{patientFirstName\}\}/g, patientFirstName);
            subject = subject.replace(/\{\{partnerFirstName\}\}/g, partnerFirstNameForSubject);
            subject = subject.replace(/\{\{patientName\}\}/g, currentEmailPatient.patientName || '');
            document.getElementById('previewSubject').textContent = subject;
            
            // Replace placeholders in body
            var body = template.body;
			
			// Handle noShow template - select correct body
			if (emailType === 'noShow') {
				var noShowType = document.getElementById('emailNoShowType');
				if (noShowType && noShowType.value === 'lateCancellation') {
					body = template.bodyLateCancellation || template.body;
				} else {
					body = template.bodyNoShow || template.body;
				}
			}
			
            body = body.replace(/\{\{greeting\}\}/g, greeting);
            body = body.replace(/\{\{patientFirstName\}\}/g, patientFirstName);
            body = body.replace(/\{\{patientName\}\}/g, currentEmailPatient.patientName || '');
            body = body.replace(/\{\{partnerName\}\}/g, currentEmailPatient.partnerName || '');
            body = body.replace(/\{\{clinicType\}\}/g, clinicType);
            body = body.replace(/\{\{appointmentType\}\}/g, apptType);
            body = body.replace(/\{\{appointmentDay\}\}/g, appointmentDay);
            body = body.replace(/\{\{appointmentDate\}\}/g, formattedDate);
            body = body.replace(/\{\{appointmentTime\}\}/g, apptTime);
            body = body.replace(/\{\{appointmentMonth\}\}/g, appointmentMonth);
            body = body.replace(/\{\{locationInfo\}\}/g, locationInfo);
            body = body.replace(/\{\{location\}\}/g, location);
            body = body.replace(/\{\{portalPassword\}\}/g, portalPassword);
            body = body.replace(/\{\{rescheduleReason\}\}/g, rescheduleReason);
            body = body.replace(/\{\{testsIncompleteBlock\}\}/g, testsIncompleteBlock);
            body = body.replace(/\{\{cancellationReason\}\}/g, cancellationReason);
            body = body.replace(/\{\{signature\}\}/g, emailTemplates.settings.signature.replace(/\n/g, '<br>'));
            body = body.replace(/\{\{appointmentDetails\}\}/g, appointmentDetails);
            
            // Replace common blocks
            body = body.replace(/\{\{locationsLink\}\}/g, emailTemplates.commonBlocks.locationsLink || '');
            body = body.replace(/\{\{disclaimers\}\}/g, emailTemplates.commonBlocks.disclaimers || '');
            body = body.replace(/\{\{nursingContact\}\}/g, emailTemplates.commonBlocks.nursingContact || '');
            body = body.replace(/\{\{bookingContact\}\}/g, emailTemplates.commonBlocks.bookingContact || '');
			body = body.replace(/\{\{bothPartnersBlock\}\}/g, bothPartnersBlock);
            
            // Handle OTN block - from JSON
            var otnBlock = '';
			var apptIncludedCheck = document.getElementById('emailApptIncluded');
			var hasAppointment = !apptIncludedCheck || apptIncludedCheck.checked;
            if (apptType === 'Virtual (OTN)' && hasAppointment) {
                otnBlock = emailTemplates.commonBlocks.otnBlock || '';
            }
            body = body.replace(/\{\{otnBlock\}\}/g, otnBlock);

			// Cycle orders block
			var cycleOrdersBlock = '';
			if (document.getElementById('emailCycleOrders') && document.getElementById('emailCycleOrders').checked) {
				var cycleType = document.getElementById('emailCycleOrdersType').value;
				if (cycleType === 'IUI') {
					cycleOrdersBlock = emailTemplates.commonBlocks.cycleOrdersIUI || '';
				} else if (cycleType === 'IVF') {
					cycleOrdersBlock = emailTemplates.commonBlocks.cycleOrdersIVF || '';
				} else if (cycleType === 'ovulation') {
					cycleOrdersBlock = emailTemplates.commonBlocks.cycleOrdersOvulation || '';
				} else if (cycleType === 'embryo') {
					cycleOrdersBlock = emailTemplates.commonBlocks.cycleOrdersEmbryo || '';
				} else {
					cycleOrdersBlock = emailTemplates.commonBlocks.cycleOrdersGeneric || '';
				}
			}
			body = body.replace(/\{\{cycleOrdersBlock\}\}/g, cycleOrdersBlock);
            
            // Handle appointment block for welcome, followUp and abortion email
            var apptIncluded = document.getElementById('emailApptIncluded');
            if (emailType === 'welcome' || emailType === 'abortion' || emailType === 'followUp') {
                if (apptIncluded && !apptIncluded.checked) {
                    body = body.replace(/\{\{appointmentBlock\}\}/g, template.appointmentPendingBlock || '');
                } else {
                    var apptBlock = template.appointmentIncludedBlock || '';
                    apptBlock = apptBlock.replace(/\{\{appointmentType\}\}/g, apptType);
                    apptBlock = apptBlock.replace(/\{\{clinicType\}\}/g, clinicType);
                    apptBlock = apptBlock.replace(/\{\{appointmentDay\}\}/g, appointmentDay);
                    apptBlock = apptBlock.replace(/\{\{appointmentDate\}\}/g, formattedDate);
                    apptBlock = apptBlock.replace(/\{\{appointmentTime\}\}/g, apptTime);
                    apptBlock = apptBlock.replace(/\{\{locationInfo\}\}/g, locationInfo);
                    apptBlock = apptBlock.replace(/\{\{appointmentDetails\}\}/g, appointmentDetails);
					apptBlock = apptBlock.replace(/\{\{otnBlock\}\}/g, otnBlock);
					apptBlock = apptBlock.replace(/\{\{locationsLink\}\}/g, emailTemplates.commonBlocks.locationsLink || '');
                    body = body.replace(/\{\{appointmentBlock\}\}/g, apptBlock);
                }
            }
            
            // Handle tests block
            var testsHtml = buildTestsBlock();
            body = body.replace(/\{\{testsBlock\}\}/g, testsHtml);

            // Handle testsIntroBlock - only show if tests are selected
            var testsIntroBlock = '';
            if (testsHtml !== '') {
                testsIntroBlock = emailTemplates.commonBlocks.testsIntroBlock || '';
            }
            body = body.replace(/\{\{testsIntroBlock\}\}/g, testsIntroBlock);
            
            // Handle handouts block - from JSON
            var handoutsBlock = '';
            if (document.getElementById('emailHandouts') && document.getElementById('emailHandouts').checked) {
                handoutsBlock = emailTemplates.commonBlocks.handoutsBlock || '';
            }
            body = body.replace(/\{\{handoutsBlock\}\}/g, handoutsBlock);
            
            // Handle referrals block - from JSON
            var referralsBlock = '';
            if (document.getElementById('emailReferral') && document.getElementById('emailReferral').checked) {
                referralsBlock = emailTemplates.commonBlocks.referralsBlock || '';
            }
            body = body.replace(/\{\{referralsBlock\}\}/g, referralsBlock);
            
            // Handle funding block - from JSON
            var fundingBlock = '';
            if (document.getElementById('emailOFP') && document.getElementById('emailOFP').checked) {
                fundingBlock = emailTemplates.commonBlocks.fundingBlock || '';
            }
            body = body.replace(/\{\{fundingBlock\}\}/g, fundingBlock);
            
            // Handle treatment block - from JSON (kept for backward compatibility)
            var treatmentBlock = '';
            if (document.getElementById('emailTreatment') && document.getElementById('emailTreatment').checked) {
                treatmentBlock = emailTemplates.commonBlocks.treatmentBlock || '';
            }
            body = body.replace(/\{\{treatmentBlock\}\}/g, treatmentBlock);
            
            body = body.replace(/\{\{appointmentDateBlock\}\}/g, formattedDate ? ' on ' + formattedDate : '');
            
            document.getElementById('previewBody').innerHTML = body;
        }
        
        // Build tests block from checkboxes
        function buildTestsBlock() {
            var html = '';
            var patientTests = [];
            var partnerTests = [];
            
            // Helper function to build tests for a person
            function buildTestsForPerson(prefix, personName) {
                var tests = [];
                var hasBloodWork = false;
                var bloodWorkTypes = [];
                var hasAMH = false;
                
                // Check blood work options
                if (document.getElementById(prefix + 'Viral') && document.getElementById(prefix + 'Viral').checked) {
                    bloodWorkTypes.push('Viral Serology');
                    hasBloodWork = true;
                }
                if (document.getElementById(prefix + 'AMH') && document.getElementById(prefix + 'AMH').checked) {
                    bloodWorkTypes.push('AMH');
                    hasBloodWork = true;
                    hasAMH = true;
                }
                if (document.getElementById(prefix + 'Other') && document.getElementById(prefix + 'Other').checked) {
                    bloodWorkTypes.push('Other (RPL, PCOS panels, other)');
                    hasBloodWork = true;
                }
                
                // Build blood work section if any selected
                if (hasBloodWork) {
                    var bloodWorkHeader = emailTemplates.tests.bloodWork.header;
                    var typesStr = bloodWorkTypes.length > 0 ? ' - ' + bloodWorkTypes.join(', ') : '';
                    bloodWorkHeader = bloodWorkHeader.replace(/\{\{bloodWorkTypes\}\}/g, typesStr);
                    
                    // Add AMH note if AMH is selected
                    if (hasAMH) {
                        bloodWorkHeader = bloodWorkHeader.replace(/\{\{amhNote\}\}/g, emailTemplates.tests.amhNote);
                    } else {
                        bloodWorkHeader = bloodWorkHeader.replace(/\{\{amhNote\}\}/g, '');
                    }
                    tests.push(bloodWorkHeader);
                }
                
                // Check ultrasound
                if (document.getElementById(prefix + 'Ultrasound') && document.getElementById(prefix + 'Ultrasound').checked) {
                    tests.push(emailTemplates.tests.ultrasound.instructions);
                }
                
                // Check sonohysterogram MSF
                if (document.getElementById(prefix + 'SonoMSF') && document.getElementById(prefix + 'SonoMSF').checked) {
                    tests.push(emailTemplates.tests.sonoMSF.instructions);
                }
                
                // Check sonohysterogram TNI
                if (document.getElementById(prefix + 'SonoTNI') && document.getElementById(prefix + 'SonoTNI').checked) {
                    tests.push(emailTemplates.tests.sonoTNI.instructions);
                }

				// Check office hysteroscopy
				if (document.getElementById(prefix + 'OfficeHysteroscopy') && document.getElementById(prefix + 'OfficeHysteroscopy').checked) {
					tests.push(emailTemplates.tests.officeHysteroscopy.instructions);
				}
				
				// Check operative hysteroscopy
				if (document.getElementById(prefix + 'OperativeHysteroscopy') && document.getElementById(prefix + 'OperativeHysteroscopy').checked) {
					tests.push(emailTemplates.tests.operativeHysteroscopy.instructions);
				}
				
                // Check semen analysis
                if (document.getElementById(prefix + 'Sperm') && document.getElementById(prefix + 'Sperm').checked) {
                    tests.push(emailTemplates.tests.semenAnalysis.instructions);
                }
                
                return tests;
            }
            
            // Check patient tests
            if (document.getElementById('emailTestsPatient') && document.getElementById('emailTestsPatient').checked) {
                patientTests = buildTestsForPerson('testPatient', currentEmailPatient.patientName);
            }
            
            // Check partner tests
            if (document.getElementById('emailTestsPartner') && document.getElementById('emailTestsPartner').checked) {
                partnerTests = buildTestsForPerson('testPartner', currentEmailPatient.partnerName);
            }
            
            if (patientTests.length > 0 || partnerTests.length > 0) {
                html += emailTemplates.commonBlocks.testsHeader || '<p><strong>The following tests are requested:</strong></p>';
                
                if (patientTests.length > 0) {
                    var patientLabel = (emailTemplates.commonBlocks.testsForPatient || '<p><strong>For {{patientFirstName}}:</strong></p>');
					patientLabel = patientLabel.replace(/\{\{patientFirstName\}\}/g, getFirstName(currentEmailPatient.patientName, currentEmailPatient.patientAlias, currentEmailPatient.patientFirstName));
                    html += patientLabel + '<ul>' + patientTests.join('') + '</ul>';
                }
                
                if (partnerTests.length > 0) {
                    var partnerLabel = (emailTemplates.commonBlocks.testsForPartner || '<p><strong>For {{partnerFirstName}}:</strong></p>');
					partnerLabel = partnerLabel.replace(/\{\{partnerFirstName\}\}/g, getFirstName(currentEmailPatient.partnerName, currentEmailPatient.partnerAlias, currentEmailPatient.partnerFirstName));
                    html += partnerLabel + '<ul>' + partnerTests.join('') + '</ul>';
                }
            }
            
            return html;

        }
        
        // Get first name from full name
		function getFirstName(fullName, alias, firstName) {
            // Use alias if provided
            if (alias && alias.trim()) {
                return alias.trim();
            }
            // Use firstName if provided (new data structure)
            if (firstName && firstName.trim()) {
                return firstName.trim();
            }
            // Fall back to parsing fullName (backward compatibility)
            if (!fullName) return '';
            // Handle "LAST, FIRST MIDDLE" format
            var parts = fullName.split(',');
            if (parts.length > 1) {
                var firstMiddle = parts[1].trim().split(' ');
                return firstMiddle[0];
            }
            // Handle "FIRST LAST" format
            return fullName.split(' ')[0];
        }
        
        // Copy email to clipboard
        function copyEmailToClipboard() {
            var subject = document.getElementById('previewSubject').textContent;
            var body = document.getElementById('previewBody').innerText;
            
            var textToCopy = 'Subject: ' + subject + '\n\n' + body;
            
            // Create temporary textarea
            var textarea = document.createElement('textarea');
            textarea.value = textToCopy;
            document.body.appendChild(textarea);
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
            
            showErrorModal('Email copied to clipboard!');
        }
        
		async function generateAndOpenOutlook() {
            var emailType = document.getElementById('emailType').value;
            if (!emailType) {
                showErrorModal('Please select an email type');
                return;
            }
            
            // Get email details
            var toList = [];
            var toPatient = document.getElementById('emailToPatient').checked;
            var toPartner = document.getElementById('emailToPartner').checked;
            
            if (toPatient && (currentEmailPatient.patientEmail || currentEmailPatient.email)) {
                toList.push(currentEmailPatient.patientEmail || currentEmailPatient.email);
            }
            if (toPartner && currentEmailPatient.partnerEmail) {
                toList.push(currentEmailPatient.partnerEmail);
            }
            
            if (toList.length === 0) {
                showErrorModal('No email addresses available for selected recipients');
                return;
            }
            
            var to = toList.join('; ');
            var subject = document.getElementById('previewSubject').textContent;
            var bodyHtml = document.getElementById('previewBody').innerHTML;
            
            // Save as pending email
            var emailsArray = [{
                to: to,
                subject: subject,
                body: bodyHtml,
                account: EMAIL_FROM_ADDRESS
            }];
            
            await saveEmailsToFile(emailsArray);
			closeModal('emailGeneratorModal');
        }

        // ============================================================================
        // GLOBAL ERROR HANDLER - Must be first to catch all errors
        // ============================================================================
        window.onerror = function(message, source, lineno, colno, error) {
            // Suppress the Flatpickr cross-origin "Script error" which doesn't actually break functionality
            // Cross-origin errors typically have "Script error." message and line 0
            if (message === 'Script error.' && lineno === 0) {
                return true;
            }
            // For other errors, show them normally
            return false;
        };
		


// ============================================================================
// EEL INTEGRATION HELPERS
// ============================================================================

// Helper: Save patient updates to backend
async function savePatientToBackend(patient) {
    try {
        if (patient.notes) {
            await eel.update_patient_notes(patient.patientID, patient.notes)();
        }
        return true;
    } catch (error) {
        console.error('Error saving patient to backend:', error);
        return false;
    }
}

// Helper: Update patient state with backend save
async function updatePatientStateWithSave(patientID, newState, notes) {
    startTiming('updatePatientStateWithSave_inner');
    
    var patient = patients.find(p => p.patientID === patientID);
    if (!patient) {
        endTiming('updatePatientStateWithSave_inner');
        return false;
    }
    
    // Get last known timestamp for conflict detection
    var lastTimestamp = null;
    if (patient.stateHistory && patient.stateHistory.length > 0) {
        lastTimestamp = patient.stateHistory[patient.stateHistory.length - 1].timestamp;
    }
    
    try {
        startTiming('eel.update_patient_state_with_version');
        var startTime = performance.now();
        
        var result = await eel.update_patient_state_with_version(
            patientID, 
            newState, 
            notes,
            lastTimestamp
        )();
        
        var elapsed = performance.now() - startTime;
        endTiming('eel.update_patient_state_with_version');
        
        // Database lock detection: If took > 1 second, another user was writing
        if (elapsed > 1000) {
            console.log('⚠️ Database was locked for ' + Math.round(elapsed) + 'ms (another user was saving)');
            if (DEBUG_TIMING) {
                logTiming('Database lock wait: ' + Math.round(elapsed) + 'ms');
            }
        }
        
        // Conflict detected!
        if (result.conflict) {
            var retry = confirm(
                'This patient was modified by another user.\n\n' +
                'Current state: ' + result.current_state + '\n\n' +
                'Do you want to override their changes?'
            );
            
            if (retry) {
                // User wants to override - use current timestamp
                var currentTimestamp = result.patient.stateHistory[result.patient.stateHistory.length - 1].timestamp;
                result = await eel.update_patient_state_with_version(
                    patientID, 
                    newState, 
                    notes,
                    currentTimestamp
                )();
            } else {
                // User cancelled - update local copy with server version
                for (var i = 0; i < patients.length; i++) {
                    if (patients[i].patientID === patientID) {
                        patients[i] = result.patient;
                        break;
                    }
                }
                endTiming('updatePatientStateWithSave_inner');
                return false;
            }
        }
        
        // Update succeeded - refresh ONLY this patient (not all 1,306!)
        if (result.success) {
            startTiming('eel.get_patient');
            var updatedPatient = await eel.get_patient(patientID)();
            endTiming('eel.get_patient');
            
            // Update in local patients array
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patients[i] = updatedPatient;
                    break;
                }
            }
        }
        
        endTiming('updatePatientStateWithSave_inner');
        return result.success;
        
    } catch (error) {
        console.error('Error updating patient state:', error);
        endTiming('updatePatientStateWithSave_inner');
        return false;
    }
}


// Helper: Schedule appointment with backend save
async function scheduleAppointmentWithSave(patientID, date, time, location) {
    startTiming('scheduleAppointmentWithSave');
    
    var patient = patients.find(p => p.patientID === patientID);
    if (!patient) {
        endTiming('scheduleAppointmentWithSave');
        return false;
    }
    
    // Lock to prevent auto-refresh collision
    isManualOperationInProgress = true;
    
    try {
        // Use conflict-checking version to prevent double-booking
        startTiming('eel.schedule_appointment_with_conflict_check');
        var result = await eel.schedule_appointment_with_conflict_check(patientID, date, time, location)();
        endTiming('eel.schedule_appointment_with_conflict_check');
        
        if (result.conflict) {
            // Time slot was taken by another user!
            var conflictingName = result.conflicting_patient.patientName;
            
            showErrorModal(
                'This time slot is no longer available!\n\n' +
                conflictingName + ' was just scheduled for:\n' +
                date + ' at ' + time + '\n\n' +
                'Please choose a different time slot.'
            );
            
            // Refresh appointments to show the conflict
            await renderAppointments();
            
            endTiming('scheduleAppointmentWithSave');
            isManualOperationInProgress = false;
            return false;
        }
        
        // Success! Update local patient with server version
        if (result.success) {
            for (var i = 0; i < patients.length; i++) {
                if (patients[i].patientID === patientID) {
                    patients[i] = result.patient;
                    break;
                }
            }
            
            // Refresh UI
            renderPatientList();
            await renderAppointments();
            updateStatusCounts();
        }
        
        endTiming('scheduleAppointmentWithSave');
        isManualOperationInProgress = false;
        return result.success;
        
    } catch (error) {
        console.error('Error scheduling appointment:', error);
        showErrorModal('Error scheduling appointment: ' + error);
        endTiming('scheduleAppointmentWithSave');
        isManualOperationInProgress = false;
        return false;
    }
}

console.log('Eel integration helpers loaded');


// ============================================================================
// PROFESSIONAL ERROR HANDLING
// ============================================================================

function showErrorModal(message) {
    var modal = document.getElementById('errorModal');
    if (modal) {
        document.getElementById('errorModalMessage').textContent = message;
        modal.style.display = 'flex';  // Use flex for centering
    } else {
        // Fallback if modal doesn't exist
        alert(message);
    }
}

function closeErrorModal() {
    var modal = document.getElementById('errorModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// Smart alert override - use modal for errors, keep confirm working
var originalAlert = window.alert;
var originalConfirm = window.confirm;

window.alert = function(message) {
    if (!message) return;
    
    var isError = message.toLowerCase().includes('error') || 
                  message.toLowerCase().includes('warning') ||
                  message.toLowerCase().includes('failed') ||
                  message.toLowerCase().includes('could not');
    
    if (isError) {
        showErrorModal(message);
    } else {
        originalAlert(message);
    }
};

window.confirm = originalConfirm;

console.log('Professional error handling loaded');
