import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  applyActionCode,
  createUserWithEmailAndPassword,
  fetchSignInMethodsForEmail,
  getAuth,
  getRedirectResult,
  GoogleAuthProvider,
  onAuthStateChanged,
  sendEmailVerification,
  sendPasswordResetEmail,
  signInWithEmailAndPassword,
  signInWithRedirect,
  signOut,
  updateProfile,
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js";
import { get, getDatabase, onValue, push, ref, remove, set, update } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js";

const firebaseConfig = {
  apiKey: "AIzaSyDAqHm_g9FO0uDIlj6P_k5KZ-mgDUGHXGc",
  authDomain: "deliverytracker-18897.firebaseapp.com",
  databaseURL: "https://deliverytracker-18897-default-rtdb.firebaseio.com",
  projectId: "deliverytracker-18897",
  storageBucket: "deliverytracker-18897.firebasestorage.app",
  messagingSenderId: "1073554045758",
  appId: "1:1073554045758:web:c5e58d19c7fdb4665e8a6f",
  measurementId: "G-XWV4G3XBPN",
};

const firebaseApp = initializeApp(firebaseConfig);
const auth = getAuth(firebaseApp);
const db = getDatabase(firebaseApp);
const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({ prompt: "select_account" });

const numberFormat = new Intl.NumberFormat("ar-EG");

const views = {
  auth: document.getElementById("auth-view"),
  verifyCode: document.getElementById("verify-code-view"),
  home: document.getElementById("home-view"),
  storyEditor: document.getElementById("story-editor-view"),
  novelOverview: document.getElementById("novel-overview-view"),
  chapterEditor: document.getElementById("chapter-editor-view"),
};

const refs = {
  loginModeBtn: document.getElementById("login-mode-btn"),
  signupModeBtn: document.getElementById("signup-mode-btn"),
  authForm: document.getElementById("auth-form"),
  emailWrap: document.getElementById("email-wrap"),
  authEmail: document.getElementById("auth-email"),
  emailCheckHint: document.getElementById("email-check-hint"),
  authIdentifierLabel: document.getElementById("auth-identifier-label"),
  authUsername: document.getElementById("auth-username"),
  usernameCheckHint: document.getElementById("username-check-hint"),
  authPassword: document.getElementById("auth-password"),
  authConfirm: document.getElementById("auth-confirm"),
  confirmWrap: document.getElementById("confirm-wrap"),
  authSubmit: document.getElementById("auth-submit"),
  googleLoginBtn: document.getElementById("google-login-btn"),
  forgotPasswordBtn: document.getElementById("forgot-password-btn"),
  authStatus: document.getElementById("auth-status"),
  verifySubtitle: document.getElementById("verify-subtitle"),
  verifyCodeInput: document.getElementById("verify-code-input"),
  verifyCodeSubmitBtn: document.getElementById("verify-code-submit-btn"),
  verifyCodeResendBtn: document.getElementById("verify-code-resend-btn"),
  verifyBackLoginBtn: document.getElementById("verify-back-login-btn"),
  verifyStatus: document.getElementById("verify-status"),
  welcomeText: document.getElementById("welcome-text"),
  homeEmpty: document.getElementById("home-empty"),
  libraryList: document.getElementById("library-list"),
  storyTitleInput: document.getElementById("story-title-input"),
  storyTextarea: document.getElementById("story-textarea"),
  storyWordCount: document.getElementById("story-word-count"),
  storySceneCount: document.getElementById("story-scene-count"),
  storySaveState: document.getElementById("story-save-state"),
  storySaveIcon: document.getElementById("story-save-icon"),
  storySaveText: document.getElementById("story-save-text"),
  novelTitleInput: document.getElementById("novel-title-input"),
  novelChapterCount: document.getElementById("novel-chapter-count"),
  novelWordCount: document.getElementById("novel-word-count"),
  novelSceneCount: document.getElementById("novel-scene-count"),
  chaptersList: document.getElementById("chapters-list"),
  chapterTitleInput: document.getElementById("chapter-title-input"),
  chapterTextarea: document.getElementById("chapter-textarea"),
  chapterWordCount: document.getElementById("chapter-word-count"),
  chapterSceneCount: document.getElementById("chapter-scene-count"),
  chapterSaveState: document.getElementById("chapter-save-state"),
  chapterSaveIcon: document.getElementById("chapter-save-icon"),
  chapterSaveText: document.getElementById("chapter-save-text"),
  toast: document.getElementById("toast"),
  pdfRoot: document.getElementById("pdf-render-root"),
};

const state = {
  authMode: "login",
  user: null,
  username: "",
  library: [],
  currentView: "auth",
  currentStoryId: null,
  currentNovelId: null,
  currentChapterId: null,
  pendingVerificationEmail: "",
  libraryUnsubscribe: null,
};

const saveTimers = {};
const saveInFlight = {};
const saveQueued = {};
const saveTasks = {};
const saveIndicators = {};
const availabilityTimers = {};
const availabilityTokens = { email: 0, username: 0 };
const usernameIndex = new Set();
let usernameIndexPromise = null;
let toastTimer = null;

bindUI();
setAuthMode("login");
showView("auth");
checkRedirectResult();

onAuthStateChanged(auth, async (user) => {
  if (state.libraryUnsubscribe) {
    state.libraryUnsubscribe();
    state.libraryUnsubscribe = null;
  }

  state.user = user;
  state.currentStoryId = null;
  state.currentNovelId = null;
  state.currentChapterId = null;

  if (!user) {
    state.username = "";
    state.library = [];
    state.pendingVerificationEmail = "";
    renderHome();
    refs.authPassword.value = "";
    refs.authConfirm.value = "";
    refs.verifyCodeInput.value = "";
    showView("auth");
    return;
  }

  try {
    if (isPasswordUser(user) && !user.emailVerified) {
      openVerifyCodeView(user.email || state.pendingVerificationEmail || "");
      return;
    }

    await ensureUserProfile(user);

    const profileSnap = await get(ref(db, `users/${user.uid}/profile`));
    const profile = profileSnap.exists() ? profileSnap.val() : {};
    state.username = user.displayName || profile.username || emailToDefaultUsername(user.email) || "كاتب";
    refs.welcomeText.textContent = `مرحبًا ${state.username}`;

    const libraryRef = ref(db, `users/${user.uid}/library`);
    state.libraryUnsubscribe = onValue(
      libraryRef,
      (snapshot) => {
        const data = snapshot.val() || {};
        state.library = Object.entries(data)
          .map(([id, item]) => ({ ...item, id: item?.id || id }))
          .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));

        if (state.currentView === "home") {
          renderHome();
        }
        if (state.currentView === "novelOverview") {
          renderNovelOverview();
        }
      },
      (error) => {
        console.error("Library subscription failed:", error);
        showToast(withErrorCode("تعذر تحميل المكتبة.", error), true);
      }
    );

    showView("home");
    renderHome();
  } catch (error) {
    console.error("Auth initialization error:", error);
    state.username = user.displayName || emailToDefaultUsername(user.email) || "كاتب";
    refs.welcomeText.textContent = `مرحبًا ${state.username}`;
    showView("home");
    renderHome();
    showToast(withErrorCode("تم تسجيل الدخول لكن تعذر تحميل بيانات القاعدة.", error), true);
  }
});

function bindUI() {
  refs.loginModeBtn.addEventListener("click", () => setAuthMode("login"));
  refs.signupModeBtn.addEventListener("click", () => setAuthMode("signup"));
  refs.authForm.addEventListener("submit", handleAuthSubmit);
  refs.googleLoginBtn.addEventListener("click", handleGoogleSignIn);
  refs.forgotPasswordBtn.addEventListener("click", handleForgotPassword);
  refs.authEmail.addEventListener("input", onSignupEmailTyping);
  refs.authUsername.addEventListener("input", onSignupUsernameTyping);
  refs.verifyCodeSubmitBtn.addEventListener("click", handleVerifyCodeSubmit);
  refs.verifyCodeResendBtn.addEventListener("click", handleResendVerificationCode);
  refs.verifyBackLoginBtn.addEventListener("click", handleVerifyBackToLogin);
  refs.verifyCodeInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void handleVerifyCodeSubmit();
    }
  });

  refs.storyTitleInput.addEventListener("input", () => {
    scheduleSave("story", saveCurrentStory, 90, "story");
  });
  refs.storyTextarea.addEventListener("input", () => {
    updateStoryStats();
    scheduleSave("story", saveCurrentStory, 90, "story");
  });

  refs.novelTitleInput.addEventListener("input", () => {
    scheduleSave("novel-title", saveCurrentNovelTitle);
  });

  refs.chapterTitleInput.addEventListener("input", () => {
    scheduleSave("chapter", saveCurrentChapter, 90, "chapter");
  });
  refs.chapterTextarea.addEventListener("input", () => {
    updateChapterStats();
    scheduleSave("chapter", saveCurrentChapter, 90, "chapter");
  });

  document.addEventListener("click", (event) => {
    const toggleBtn = event.target.closest("[data-toggle-menu]");
    if (toggleBtn) {
      event.preventDefault();
      toggleMenu(toggleBtn.dataset.toggleMenu);
      return;
    }

    const actionBtn = event.target.closest("[data-action]");
    if (actionBtn) {
      event.preventDefault();
      closeAllMenus();
      handleAction(actionBtn.dataset.action, actionBtn.dataset).catch((error) => {
        console.error(error);
        showToast(readableError(error), true);
      });
      return;
    }

    if (!event.target.closest(".menu-wrap")) {
      closeAllMenus();
    }
  });
}

function setAuthMode(mode) {
  state.authMode = mode;
  const signUpMode = mode === "signup";
  refs.loginModeBtn.classList.toggle("active", !signUpMode);
  refs.signupModeBtn.classList.toggle("active", signUpMode);
  refs.emailWrap.classList.toggle("hidden", !signUpMode);
  refs.confirmWrap.classList.toggle("hidden", !signUpMode);
  refs.forgotPasswordBtn.classList.toggle("hidden", signUpMode);
  refs.emailCheckHint.classList.toggle("hidden", !signUpMode);
  refs.usernameCheckHint.classList.toggle("hidden", !signUpMode);
  refs.authIdentifierLabel.textContent = signUpMode ? "اسم المستخدم" : "اسم المستخدم أو الايميل";
  refs.authUsername.placeholder = signUpMode ? "اسم المستخدم" : "اسم المستخدم أو الايميل";
  refs.authSubmit.textContent = signUpMode ? "إنشاء الحساب" : "دخول";
  refs.authStatus.textContent = "";
  refs.verifyStatus.textContent = "";

  if (signUpMode) {
    renderFieldCheck("email", "idle", "اكتب بريدك للتحقق من توفره.");
    renderFieldCheck("username", "idle", "اكتب اسم المستخدم للتحقق من توفره.");
  } else {
    refs.emailCheckHint.textContent = "";
    refs.usernameCheckHint.textContent = "";
  }
}

async function handleAuthSubmit(event) {
  event.preventDefault();

  const identifier = refs.authUsername.value.trim();
  const emailInput = refs.authEmail.value.trim();
  const password = refs.authPassword.value;
  const confirm = refs.authConfirm.value;

  if (password.length < 8) {
    refs.authStatus.textContent = "كلمة المرور يجب ألا تقل عن 8 خانات.";
    return;
  }

  refs.authStatus.style.color = "#0f4c49";
  refs.authStatus.textContent = state.authMode === "signup" ? "جارٍ إنشاء الحساب..." : "جارٍ تسجيل الدخول...";
  refs.authSubmit.disabled = true;
  refs.googleLoginBtn.disabled = true;

  try {
    if (state.authMode === "signup") {
      if (identifier.length < 3) {
        throw new Error("اسم المستخدم يجب أن يكون 3 أحرف على الأقل.");
      }
      if (!isValidEmail(emailInput)) {
        throw new Error("اكتب بريدًا إلكترونيًا صحيحًا.");
      }
      if (password !== confirm) {
        throw new Error("تأكيد كلمة المرور غير متطابق.");
      }
      const emailAvailable = await isEmailAvailable(emailInput);
      if (!emailAvailable) {
        renderFieldCheck("email", "bad", "هذا البريد مستخدم بالفعل.");
        throw new Error("هذا البريد مستخدم بالفعل.");
      }
      const usernameAvailable = await isUsernameAvailable(identifier);
      if (usernameAvailable === false) {
        renderFieldCheck("username", "bad", "اسم المستخدم مستخدم بالفعل.");
        throw new Error("اسم المستخدم مستخدم بالفعل.");
      }
      if (usernameAvailable === null) {
        throw new Error("تعذر التأكد من اسم المستخدم الآن. حاول بعد ثوانٍ.");
      }

      await signUpWithUsername(identifier, emailInput, password);
      refs.authPassword.value = "";
      refs.authConfirm.value = "";
      openVerifyCodeView(emailInput.toLowerCase());
      refs.verifyStatus.style.color = "#0f4c49";
      refs.verifyStatus.textContent = "أرسلنا لك رسالة التحقق. الصق الرمز هنا بعد استلامه.";
    } else {
      if (!identifier) {
        throw new Error("اكتب اسم المستخدم أو البريد الإلكتروني.");
      }
      const result = await loginWithIdentifier(identifier, password);
      if (result?.needsVerification) {
        openVerifyCodeView(result.email || "");
        refs.verifyStatus.style.color = "#0f4c49";
        refs.verifyStatus.textContent = "حسابك غير موثق بعد. أعدنا إرسال رمز جديد.";
      } else {
        refs.authStatus.textContent = "تم تسجيل الدخول.";
      }
    }
  } catch (error) {
    refs.authStatus.style.color = "#b3261e";
    refs.authStatus.textContent = withErrorCode(readableError(error), error);
  } finally {
    refs.authSubmit.disabled = false;
    refs.googleLoginBtn.disabled = false;
  }
}

async function handleGoogleSignIn() {
  refs.authStatus.style.color = "#0f4c49";
  refs.authStatus.textContent = "جارٍ التحويل إلى Google...";
  refs.googleLoginBtn.disabled = true;
  refs.authSubmit.disabled = true;

  try {
    await signInWithRedirect(auth, googleProvider);
  } catch (error) {
    refs.authStatus.style.color = "#b3261e";
    refs.authStatus.textContent = withErrorCode(readableError(error), error);
    console.error("Google sign-in failed:", error);
  } finally {
    refs.googleLoginBtn.disabled = false;
    refs.authSubmit.disabled = false;
  }
}

async function handleForgotPassword() {
  const seeded = refs.authUsername.value.trim();
  const input = window.prompt("اكتب البريد الإلكتروني لاستعادة كلمة المرور:", seeded.includes("@") ? seeded : "");
  if (input === null) return;

  const email = input.trim().toLowerCase();
  if (!isValidEmail(email)) {
    showToast("اكتب بريدًا إلكترونيًا صحيحًا.", true);
    return;
  }

  try {
    await sendPasswordResetEmail(auth, email);
    showToast("تم إرسال رابط إعادة تعيين كلمة المرور إلى بريدك.");
  } catch (error) {
    showToast(withErrorCode(readableError(error), error), true);
  }
}

function onSignupEmailTyping() {
  if (state.authMode !== "signup") return;
  const email = refs.authEmail.value.trim().toLowerCase();
  clearTimeout(availabilityTimers.email);

  if (!email) {
    renderFieldCheck("email", "idle", "اكتب بريدك للتحقق من توفره.");
    return;
  }
  if (!isValidEmail(email)) {
    renderFieldCheck("email", "bad", "صيغة البريد غير صحيحة.");
    return;
  }

  renderFieldCheck("email", "checking", "جارٍ التحقق...");
  availabilityTimers.email = window.setTimeout(async () => {
    const token = ++availabilityTokens.email;
    try {
      const available = await isEmailAvailable(email);
      if (token !== availabilityTokens.email) return;
      renderFieldCheck("email", available ? "ok" : "bad", available ? "✓ البريد متاح." : "هذا البريد مستخدم بالفعل.");
    } catch (error) {
      if (token !== availabilityTokens.email) return;
      renderFieldCheck("email", "bad", withErrorCode("تعذر التحقق من البريد الآن.", error));
    }
  }, 320);
}

function onSignupUsernameTyping() {
  if (state.authMode !== "signup") return;
  const username = refs.authUsername.value.trim();
  clearTimeout(availabilityTimers.username);

  if (!username) {
    renderFieldCheck("username", "idle", "اكتب اسم المستخدم للتحقق من توفره.");
    return;
  }
  if (username.length < 3) {
    renderFieldCheck("username", "bad", "اسم المستخدم يجب أن يكون 3 أحرف على الأقل.");
    return;
  }

  renderFieldCheck("username", "checking", "جارٍ التحقق...");
  availabilityTimers.username = window.setTimeout(async () => {
    const token = ++availabilityTokens.username;
    try {
      const available = await isUsernameAvailable(username);
      if (token !== availabilityTokens.username) return;
      if (available === true) {
        renderFieldCheck("username", "ok", "✓ اسم المستخدم متاح.");
      } else if (available === false) {
        renderFieldCheck("username", "bad", "اسم المستخدم مستخدم بالفعل.");
      } else {
        renderFieldCheck("username", "checking", "تعذر التأكد بالكامل من الاسم الآن.");
      }
    } catch (error) {
      if (token !== availabilityTokens.username) return;
      renderFieldCheck("username", "bad", withErrorCode("تعذر التحقق من اسم المستخدم الآن.", error));
    }
  }, 320);
}

function renderFieldCheck(field, status, message) {
  const target = field === "email" ? refs.emailCheckHint : refs.usernameCheckHint;
  if (!target) return;
  target.classList.remove("is-checking", "is-ok", "is-bad");
  if (status === "checking") target.classList.add("is-checking");
  if (status === "ok") target.classList.add("is-ok");
  if (status === "bad") target.classList.add("is-bad");
  target.textContent = message || "";
}

async function isEmailAvailable(email) {
  const normalized = String(email || "").trim().toLowerCase();

  try {
    const methods = await fetchSignInMethodsForEmail(auth, normalized);
    if (Array.isArray(methods) && methods.length > 0) {
      return false;
    }
  } catch (error) {
    console.warn("fetchSignInMethodsForEmail failed:", error);
  }

  const key = emailToKey(normalized);
  const snap = await get(ref(db, `emails/${key}`));
  return !snap.exists();
}

async function isUsernameAvailable(username) {
  const key = usernameToKey(username);
  const snap = await get(ref(db, `usernames/${key}`));
  if (snap.exists()) {
    return false;
  }

  const profileIndexLoaded = await ensureUsernameIndex();
  if (profileIndexLoaded) {
    return !usernameIndex.has(normalizeUsername(username));
  }

  return null;
}

function openVerifyCodeView(email) {
  state.pendingVerificationEmail = String(email || "").trim().toLowerCase();
  refs.authStatus.textContent = "";
  refs.verifySubtitle.textContent = state.pendingVerificationEmail
    ? `أدخل رمز التحقق المرسل إلى ${state.pendingVerificationEmail}. يمكنك أيضًا لصق الرابط كاملًا وسنستخرج الرمز تلقائيًا.`
    : "أدخل رمز التحقق الذي وصلك على البريد. يمكنك أيضًا لصق الرابط كاملًا.";
  refs.verifyCodeInput.value = "";
  refs.verifyStatus.textContent = "";
  showView("verifyCode");
}

async function handleVerifyCodeSubmit() {
  const rawInput = refs.verifyCodeInput.value.trim();
  if (!rawInput) {
    refs.verifyStatus.style.color = "#b3261e";
    refs.verifyStatus.textContent = "اكتب رمز التحقق أولًا.";
    return;
  }

  refs.verifyCodeSubmitBtn.disabled = true;
  refs.verifyCodeResendBtn.disabled = true;
  refs.verifyStatus.style.color = "#0f4c49";
  refs.verifyStatus.textContent = "جارٍ التحقق من الرمز...";

  try {
    const code = extractVerificationCode(rawInput);
    await applyActionCode(auth, code);
    const verifiedEmail = auth.currentUser?.email || state.pendingVerificationEmail || "";
    if (auth.currentUser) {
      await auth.currentUser.reload();
    }
    await signOut(auth);
    setAuthMode("login");
    refs.authUsername.value = verifiedEmail;
    refs.authStatus.style.color = "#178d47";
    refs.authStatus.textContent = "تم تأكيد البريد بنجاح. سجل الدخول الآن.";
    showView("auth");
    showToast("تم تأكيد البريد بنجاح.");
  } catch (error) {
    refs.verifyStatus.style.color = "#b3261e";
    refs.verifyStatus.textContent = withErrorCode("رمز التحقق غير صحيح أو منتهي.", error);
  } finally {
    refs.verifyCodeSubmitBtn.disabled = false;
    refs.verifyCodeResendBtn.disabled = false;
  }
}

async function handleResendVerificationCode() {
  if (!auth.currentUser) {
    refs.verifyStatus.style.color = "#b3261e";
    refs.verifyStatus.textContent = "انتهت الجلسة. سجل الدخول ثم أعد الطلب.";
    return;
  }

  refs.verifyCodeResendBtn.disabled = true;
  refs.verifyStatus.style.color = "#0f4c49";
  refs.verifyStatus.textContent = "جارٍ إعادة إرسال رمز جديد...";

  try {
    await sendVerificationForCurrentUser();
    refs.verifyStatus.style.color = "#0f4c49";
    refs.verifyStatus.textContent = "تم إرسال رمز جديد. افحص الوارد والغير مرغوب فيه.";
  } catch (error) {
    refs.verifyStatus.style.color = "#b3261e";
    refs.verifyStatus.textContent = withErrorCode("تعذر إعادة إرسال الرمز.", error);
  } finally {
    refs.verifyCodeResendBtn.disabled = false;
  }
}

async function handleVerifyBackToLogin() {
  if (auth.currentUser && isPasswordUser(auth.currentUser) && !auth.currentUser.emailVerified) {
    await signOut(auth);
  }
  setAuthMode("login");
  refs.authUsername.value = state.pendingVerificationEmail || "";
  refs.authStatus.style.color = "#0f4c49";
  refs.authStatus.textContent = "أدخل بياناتك بعد تأكيد البريد.";
  showView("auth");
}

async function signUpWithUsername(username, email, password) {
  const normalizedEmail = email.trim().toLowerCase();
  const usernameKey = usernameToKey(username);
  const emailKey = emailToKey(normalizedEmail);
  const usernameRef = ref(db, `usernames/${usernameKey}`);
  const emailRef = ref(db, `emails/${emailKey}`);
  const usernameSnap = await get(usernameRef);
  const emailSnap = await get(emailRef);
  if (usernameSnap.exists()) {
    throw new Error("اسم المستخدم مستخدم بالفعل.");
  }
  if (emailSnap.exists()) {
    throw new Error("هذا البريد مستخدم بالفعل.");
  }

  const credential = await createUserWithEmailAndPassword(auth, normalizedEmail, password);
  const now = Date.now();
  const user = credential.user;

  await Promise.all([
    updateProfile(user, { displayName: username }),
    set(ref(db, `users/${user.uid}/profile`), {
      username,
      email: normalizedEmail,
      emailVerified: false,
      provider: "password",
      createdAt: now,
    }),
    set(usernameRef, { uid: user.uid, email: normalizedEmail, username, createdAt: now }),
    set(emailRef, { uid: user.uid, email: normalizedEmail, createdAt: now }),
  ]);

  usernameIndex.add(normalizeUsername(username));

  await sendVerificationForCurrentUser();
}

async function loginWithIdentifier(identifier, password) {
  const normalized = identifier.trim();
  const email = normalized.includes("@") ? normalized.toLowerCase() : await resolveEmailByUsername(normalized);
  const credential = await signInWithEmailAndPassword(auth, email, password);

  if (isPasswordUser(credential.user) && !credential.user.emailVerified) {
    try {
      await sendVerificationForCurrentUser();
    } catch (error) {
      console.error("Resend verification failed:", error);
    }
    return { needsVerification: true, email: credential.user.email || email };
  }

  if (!credential.user.displayName) {
    const profileSnap = await get(ref(db, `users/${credential.user.uid}/profile`));
    const profile = profileSnap.exists() ? profileSnap.val() : null;
    if (profile?.username) {
      await updateProfile(credential.user, { displayName: profile.username });
    }
  }

  return { needsVerification: false, email };
}

async function handleAction(action, data) {
  switch (action) {
    case "logout":
      await signOut(auth);
      return;

    case "create-story":
      await createStory();
      return;

    case "create-novel":
      await createNovel();
      return;

    case "open-item":
      await openItem(data.itemId);
      return;

    case "rename-item":
      await renameItem(data.itemId);
      return;

    case "delete-item":
      await deleteItem(data.itemId);
      return;

    case "download-item-word":
      await downloadItemAsWord(data.itemId);
      return;

    case "download-item-pdf":
      await downloadItemAsPdf(data.itemId);
      return;

    case "story-new-scene":
      insertSceneMarker(refs.storyTextarea);
      return;

    case "story-save-exit":
    case "story-back-home":
      await flushSave("story", saveCurrentStory, "story");
      goHome();
      return;

    case "story-download-word":
      downloadWordFromExport(buildStoryExport(currentStoryDraft()));
      return;

    case "story-download-pdf":
      await downloadPdfFromExport(buildStoryExport(currentStoryDraft()));
      return;

    case "novel-back-home":
      await flushSave("novel-title", saveCurrentNovelTitle);
      goHome();
      return;

    case "novel-add-chapter": {
      await flushSave("novel-title", saveCurrentNovelTitle);
      const chapterId = await addChapter(state.currentNovelId);
      await openChapterEditor(state.currentNovelId, chapterId);
      return;
    }

    case "open-chapter":
      await openChapterEditor(data.novelId, data.chapterId);
      return;

    case "novel-download-word": {
      const novel = getItemById(state.currentNovelId);
      if (!novel) return;
      const draft = { ...novel, title: refs.novelTitleInput.value.trim() || novel.title };
      downloadWordFromExport(buildNovelExport(draft));
      return;
    }

    case "novel-download-pdf": {
      const novel = getItemById(state.currentNovelId);
      if (!novel) return;
      const draft = { ...novel, title: refs.novelTitleInput.value.trim() || novel.title };
      await downloadPdfFromExport(buildNovelExport(draft));
      return;
    }

    case "novel-delete":
      await deleteCurrentNovel();
      return;

    case "chapter-new-scene":
      insertSceneMarker(refs.chapterTextarea);
      return;

    case "chapter-new-chapter": {
      await flushSave("chapter", saveCurrentChapter, "chapter");
      const chapterId = await addChapter(state.currentNovelId);
      await openChapterEditor(state.currentNovelId, chapterId);
      return;
    }

    case "chapter-save-exit":
    case "chapter-back-overview":
      await flushSave("chapter", saveCurrentChapter, "chapter");
      await openNovelOverview(state.currentNovelId);
      return;

    case "chapter-download-word":
      downloadWordFromExport(buildChapterExport(currentChapterDraft()));
      return;

    case "chapter-download-pdf":
      await downloadPdfFromExport(buildChapterExport(currentChapterDraft()));
      return;

    default:
      return;
  }
}

async function createStory() {
  ensureAuth();
  const storyId = push(ref(db, `users/${state.user.uid}/library`)).key;
  const now = Date.now();
  const index = state.library.filter((item) => item.type === "story").length + 1;

  const story = {
    id: storyId,
    type: "story",
    title: `قصة ${numberFormat.format(index)}`,
    content: "",
    createdAt: now,
    updatedAt: now,
  };

  await set(ref(db, `users/${state.user.uid}/library/${storyId}`), story);
  await openStoryEditor(storyId);
}

async function createNovel() {
  ensureAuth();
  const novelId = push(ref(db, `users/${state.user.uid}/library`)).key;
  const chapterId = push(ref(db, `users/${state.user.uid}/library/${novelId}/chapters`)).key;
  const now = Date.now();
  const index = state.library.filter((item) => item.type === "novel").length + 1;

  const firstChapter = {
    id: chapterId,
    title: defaultChapterTitle(1),
    content: "",
    order: 1,
    createdAt: now,
    updatedAt: now,
  };

  const novel = {
    id: novelId,
    type: "novel",
    title: `رواية ${numberFormat.format(index)}`,
    chapters: { [chapterId]: firstChapter },
    createdAt: now,
    updatedAt: now,
  };

  await set(ref(db, `users/${state.user.uid}/library/${novelId}`), novel);
  await openChapterEditor(novelId, chapterId);
}

async function addChapter(novelId) {
  ensureAuth();
  const novel = await ensureItemLoaded(novelId);
  if (!novel || novel.type !== "novel") {
    throw new Error("تعذر إضافة فصل.");
  }

  const chapters = getChapters(novel);
  const highestOrder = chapters.reduce((max, chapter) => Math.max(max, Number(chapter.order) || 0), 0);
  const nextOrder = highestOrder + 1 || chapters.length + 1;
  const chapterId = push(ref(db, `users/${state.user.uid}/library/${novelId}/chapters`)).key;
  const now = Date.now();

  const chapter = {
    id: chapterId,
    title: defaultChapterTitle(nextOrder),
    content: "",
    order: nextOrder,
    createdAt: now,
    updatedAt: now,
  };

  await update(ref(db, `users/${state.user.uid}/library/${novelId}`), {
    [`chapters/${chapterId}`]: chapter,
    updatedAt: now,
  });

  const localNovel = getItemById(novelId);
  if (localNovel) {
    localNovel.chapters = { ...(localNovel.chapters || {}), [chapterId]: chapter };
    localNovel.updatedAt = now;
  }

  return chapterId;
}

async function openItem(itemId) {
  const item = await ensureItemLoaded(itemId);
  if (!item) return;

  if (item.type === "story") {
    await openStoryEditor(itemId);
  } else {
    await openNovelOverview(itemId);
  }
}

async function openStoryEditor(storyId) {
  const story = await ensureItemLoaded(storyId);
  if (!story || story.type !== "story") {
    showToast("القصة غير موجودة.", true);
    return;
  }

  state.currentStoryId = storyId;
  state.currentNovelId = null;
  state.currentChapterId = null;

  refs.storyTitleInput.value = story.title || "قصة بدون عنوان";
  refs.storyTextarea.value = story.content || "";
  updateStoryStats();
  setSaveState("story", "saved");

  showView("storyEditor");
  refs.storyTextarea.focus();
}

async function openNovelOverview(novelId) {
  const novel = await ensureItemLoaded(novelId);
  if (!novel || novel.type !== "novel") {
    showToast("الرواية غير موجودة.", true);
    return;
  }

  state.currentStoryId = null;
  state.currentNovelId = novelId;
  state.currentChapterId = null;
  showView("novelOverview");
  renderNovelOverview();
}

async function openChapterEditor(novelId, chapterId) {
  const novel = await ensureItemLoaded(novelId);
  if (!novel || novel.type !== "novel") {
    showToast("تعذر فتح الفصل.", true);
    return;
  }

  const chapter = getChapters(novel).find((item) => item.id === chapterId);
  if (!chapter) {
    showToast("الفصل غير موجود.", true);
    return;
  }

  state.currentStoryId = null;
  state.currentNovelId = novelId;
  state.currentChapterId = chapterId;

  refs.chapterTitleInput.value = chapter.title || "فصل";
  refs.chapterTextarea.value = chapter.content || "";
  updateChapterStats();
  setSaveState("chapter", "saved");

  showView("chapterEditor");
  refs.chapterTextarea.focus();
}

function renderHome() {
  const items = state.library;
  refs.homeEmpty.classList.toggle("hidden", items.length > 0);

  if (items.length === 0) {
    refs.libraryList.innerHTML = "";
    return;
  }

  refs.libraryList.innerHTML = items
    .map((item) => {
      const title = escapeHtml(item.title || (item.type === "story" ? "قصة" : "رواية"));
      const stats = item.type === "story" ? getStoryStats(item) : getNovelTotals(item);
      const subtitle =
        item.type === "story"
          ? `قصة • ${numberFormat.format(stats.words)} كلمة • ${numberFormat.format(stats.scenes)} مشهد`
          : `رواية • ${numberFormat.format(stats.chapters)} فصل • ${numberFormat.format(stats.words)} كلمة • ${numberFormat.format(stats.scenes)} مشهد`;
      const menuId = `item-menu-${item.id}`;

      return `
        <article class="item-card">
          <div class="item-card-head">
            <h3>${title}</h3>
            <div class="menu-wrap">
              <button class="icon-btn small" data-toggle-menu="${menuId}" type="button" aria-label="خيارات">⋮</button>
              <div id="${menuId}" class="dropdown">
                <button data-action="rename-item" data-item-id="${item.id}" type="button">تغيير الاسم</button>
                <button data-action="download-item-word" data-item-id="${item.id}" type="button">تحميل Word</button>
                <button data-action="download-item-pdf" data-item-id="${item.id}" type="button">تحميل PDF</button>
                <button data-action="delete-item" data-item-id="${item.id}" class="danger" type="button">حذف</button>
              </div>
            </div>
          </div>
          <p class="item-meta">${subtitle}</p>
          <button class="open-btn" data-action="open-item" data-item-id="${item.id}" type="button">
            ${item.type === "story" ? "فتح القصة" : "فتح الرواية"}
          </button>
        </article>
      `;
    })
    .join("");
}

function renderNovelOverview() {
  const novel = getItemById(state.currentNovelId);
  if (!novel || novel.type !== "novel") {
    goHome();
    return;
  }

  refs.novelTitleInput.value = novel.title || "رواية بدون عنوان";

  const totals = getNovelTotals(novel);
  refs.novelChapterCount.textContent = numberFormat.format(totals.chapters);
  refs.novelWordCount.textContent = numberFormat.format(totals.words);
  refs.novelSceneCount.textContent = numberFormat.format(totals.scenes);

  const chapters = getChapters(novel);
  if (chapters.length === 0) {
    refs.chaptersList.innerHTML = `
      <article class="item-card">
        <h3>لا توجد فصول</h3>
        <p class="item-meta">اضغط على زر + لإضافة فصل جديد.</p>
      </article>
    `;
    return;
  }

  refs.chaptersList.innerHTML = chapters
    .map((chapter) => {
      const words = countWords(chapter.content || "");
      const scenes = countScenes(chapter.content || "");
      return `
        <button class="chapter-item" data-action="open-chapter" data-novel-id="${novel.id}" data-chapter-id="${chapter.id}" type="button">
          <h4>${escapeHtml(chapter.title || "فصل")}</h4>
          <p>${numberFormat.format(words)} كلمة • ${numberFormat.format(scenes)} مشهد</p>
        </button>
      `;
    })
    .join("");
}

function updateStoryStats() {
  refs.storyWordCount.textContent = numberFormat.format(countWords(refs.storyTextarea.value));
  refs.storySceneCount.textContent = numberFormat.format(countScenes(refs.storyTextarea.value));
}

function updateChapterStats() {
  refs.chapterWordCount.textContent = numberFormat.format(countWords(refs.chapterTextarea.value));
  refs.chapterSceneCount.textContent = numberFormat.format(countScenes(refs.chapterTextarea.value));
}

async function saveCurrentStory() {
  if (!state.user || !state.currentStoryId) return;
  const now = Date.now();
  await update(ref(db, `users/${state.user.uid}/library/${state.currentStoryId}`), {
    title: refs.storyTitleInput.value.trim() || "قصة بدون عنوان",
    content: refs.storyTextarea.value,
    updatedAt: now,
  });
}

async function saveCurrentNovelTitle() {
  if (!state.user || !state.currentNovelId) return;
  await update(ref(db, `users/${state.user.uid}/library/${state.currentNovelId}`), {
    title: refs.novelTitleInput.value.trim() || "رواية بدون عنوان",
    updatedAt: Date.now(),
  });
}

async function saveCurrentChapter() {
  if (!state.user || !state.currentNovelId || !state.currentChapterId) return;
  const now = Date.now();
  await Promise.all([
    update(ref(db, `users/${state.user.uid}/library/${state.currentNovelId}/chapters/${state.currentChapterId}`), {
      title: refs.chapterTitleInput.value.trim() || "فصل",
      content: refs.chapterTextarea.value,
      updatedAt: now,
    }),
    update(ref(db, `users/${state.user.uid}/library/${state.currentNovelId}`), { updatedAt: now }),
  ]);
}

async function renameItem(itemId) {
  const item = getItemById(itemId);
  if (!item || !state.user) return;

  const newName = window.prompt("اكتب الاسم الجديد:", item.title || "");
  if (newName === null) return;
  const cleaned = newName.trim();
  if (!cleaned) return;

  await update(ref(db, `users/${state.user.uid}/library/${itemId}`), {
    title: cleaned,
    updatedAt: Date.now(),
  });
  showToast("تم تغيير الاسم.");
}

async function deleteItem(itemId) {
  if (!state.user) return;
  const item = getItemById(itemId);
  if (!item) return;

  const approved = window.confirm(`هل تريد حذف "${item.title}"؟`);
  if (!approved) return;

  await remove(ref(db, `users/${state.user.uid}/library/${itemId}`));
  showToast("تم الحذف.");

  if (state.currentStoryId === itemId || state.currentNovelId === itemId) {
    goHome();
  }
}

async function deleteCurrentNovel() {
  if (!state.currentNovelId) return;
  await deleteItem(state.currentNovelId);
}

async function downloadItemAsWord(itemId) {
  const item = getItemById(itemId);
  if (!item) return;

  if (item.type === "story") {
    downloadWordFromExport(buildStoryExport(item));
    return;
  }

  downloadWordFromExport(buildNovelExport(item));
}

async function downloadItemAsPdf(itemId) {
  const item = getItemById(itemId);
  if (!item) return;

  if (item.type === "story") {
    await downloadPdfFromExport(buildStoryExport(item));
    return;
  }

  await downloadPdfFromExport(buildNovelExport(item));
}

function currentStoryDraft() {
  const story = getItemById(state.currentStoryId) || {};
  return {
    ...story,
    title: refs.storyTitleInput.value.trim() || story.title || "قصة",
    content: refs.storyTextarea.value,
  };
}

function currentChapterDraft() {
  const novel = getItemById(state.currentNovelId) || {};
  return {
    novelTitle: novel.title || "رواية",
    chapterTitle: refs.chapterTitleInput.value.trim() || "فصل",
    content: refs.chapterTextarea.value,
  };
}

function buildStoryExport(story) {
  return {
    fileName: sanitizeFileName(story.title || "قصة"),
    html: `<h1 class="export-title">${escapeHtml(story.title || "قصة")}</h1>${textToExportHtml(story.content || "")}`,
  };
}

function buildNovelExport(novel) {
  const chapters = getChapters(novel);
  const title = novel.title || "رواية";
  const body = chapters
    .map((chapter) => `<h2><strong>${escapeHtml(chapter.title || "فصل")}</strong></h2>${textToExportHtml(chapter.content || "")}`)
    .join("<p>&nbsp;</p>");

  return {
    fileName: sanitizeFileName(title),
    html: `<h1 class="export-title">${escapeHtml(title)}</h1>${body || "<p>لا يوجد محتوى.</p>"}`,
  };
}

function buildChapterExport(chapter) {
  const title = `${chapter.novelTitle} - ${chapter.chapterTitle}`;
  return {
    fileName: sanitizeFileName(title),
    html: `<h1 class="export-title">${escapeHtml(chapter.chapterTitle)}</h1>${textToExportHtml(chapter.content || "")}`,
  };
}

function downloadWordFromExport(data) {
  const doc = `<!doctype html>
<html lang="ar" dir="rtl">
  <head>
    <meta charset="UTF-8" />
    <title>${escapeHtml(data.fileName)}</title>
  </head>
  <body style="font-family: Tahoma, Arial, sans-serif; line-height: 1.9; margin: 24px;">
    ${data.html}
  </body>
</html>`;

  const blob = new Blob(["\ufeff", doc], { type: "application/msword;charset=utf-8" });
  triggerDownload(blob, `${data.fileName}.doc`);
}

async function downloadPdfFromExport(data) {
  if (!window.html2canvas || !window.jspdf) {
    showToast("مكتبات PDF غير جاهزة.", true);
    return;
  }

  refs.pdfRoot.innerHTML = `<section dir="rtl">${data.html}</section>`;
  await wait(90);

  try {
    const canvas = await window.html2canvas(refs.pdfRoot.firstElementChild, {
      scale: 2,
      useCORS: true,
      backgroundColor: "#ffffff",
    });
    const imgData = canvas.toDataURL("image/png");
    const { jsPDF } = window.jspdf;
    const pdf = new jsPDF("p", "pt", "a4");
    const pageWidth = pdf.internal.pageSize.getWidth();
    const pageHeight = pdf.internal.pageSize.getHeight();
    const margin = 22;
    const imgWidth = pageWidth - margin * 2;
    const imgHeight = (canvas.height * imgWidth) / canvas.width;
    let remaining = imgHeight;
    let y = margin;

    pdf.addImage(imgData, "PNG", margin, y, imgWidth, imgHeight, undefined, "FAST");
    remaining -= pageHeight - margin * 2;

    while (remaining > 0) {
      y = margin - (imgHeight - remaining);
      pdf.addPage();
      pdf.addImage(imgData, "PNG", margin, y, imgWidth, imgHeight, undefined, "FAST");
      remaining -= pageHeight - margin * 2;
    }

    pdf.save(`${data.fileName}.pdf`);
  } finally {
    refs.pdfRoot.innerHTML = "";
  }
}

function textToExportHtml(text) {
  const normalized = (text || "").replace(/\r/g, "");
  const lines = normalized.split("\n");
  if (lines.length === 0) return "<p> </p>";

  return lines
    .map((line) => {
      if (line.trim() === "***") {
        return '<p class="separator">***</p>';
      }
      if (!line.trim()) {
        return "<p>&nbsp;</p>";
      }
      return `<p>${escapeHtml(line)}</p>`;
    })
    .join("");
}

function insertSceneMarker(textarea) {
  const marker = "\n\n***\n\n";
  const start = textarea.selectionStart || 0;
  const end = textarea.selectionEnd || 0;
  const before = textarea.value.slice(0, start);
  const after = textarea.value.slice(end);
  textarea.value = `${before}${marker}${after}`;

  const nextCursor = start + marker.length;
  textarea.setSelectionRange(nextCursor, nextCursor);
  textarea.focus();
  textarea.dispatchEvent(new Event("input", { bubbles: true }));
}

function countWords(text) {
  const clean = (text || "").trim();
  if (!clean) return 0;
  return clean.split(/\s+/u).filter(Boolean).length;
}

function countScenes(text) {
  const lines = (text || "").replace(/\r/g, "").split("\n");
  let scenes = 0;
  let buffer = [];

  for (const line of lines) {
    if (line.trim() === "***") {
      if (buffer.join("\n").trim()) scenes += 1;
      buffer = [];
    } else {
      buffer.push(line);
    }
  }

  if (buffer.join("\n").trim()) scenes += 1;
  return scenes;
}

function getStoryStats(story) {
  return {
    words: countWords(story.content || ""),
    scenes: countScenes(story.content || ""),
  };
}

function getNovelTotals(novel) {
  const chapters = getChapters(novel);
  let words = 0;
  let scenes = 0;

  chapters.forEach((chapter) => {
    words += countWords(chapter.content || "");
    scenes += countScenes(chapter.content || "");
  });

  return { chapters: chapters.length, words, scenes };
}

function getChapters(novel) {
  return Object.entries(novel?.chapters || {})
    .map(([id, chapter]) => ({ ...chapter, id: chapter?.id || id }))
    .sort((a, b) => (Number(a.order) || 0) - (Number(b.order) || 0) || (a.createdAt || 0) - (b.createdAt || 0));
}

function defaultChapterTitle(order) {
  if (order === 1) return "الفصل الأول";
  return `الفصل ${numberFormat.format(order)}`;
}

async function ensureItemLoaded(itemId) {
  const local = getItemById(itemId);
  if (local) return local;
  if (!state.user) return null;

  const snap = await get(ref(db, `users/${state.user.uid}/library/${itemId}`));
  if (!snap.exists()) return null;
  return { ...snap.val(), id: itemId };
}

function getItemById(itemId) {
  return state.library.find((item) => item.id === itemId) || null;
}

function showView(name) {
  Object.values(views).forEach((view) => view.classList.add("hidden"));
  if (views[name]) {
    views[name].classList.remove("hidden");
    state.currentView = name;
  }
  closeAllMenus();
}

function goHome() {
  state.currentStoryId = null;
  state.currentNovelId = null;
  state.currentChapterId = null;
  showView("home");
  renderHome();
}

function scheduleSave(key, task, delay = 700, indicator = null) {
  saveTasks[key] = task;
  if (indicator) {
    saveIndicators[key] = indicator;
    setSaveState(indicator, "saving");
  }
  clearTimeout(saveTimers[key]);
  saveTimers[key] = window.setTimeout(() => {
    void processSave(key);
  }, delay);
}

async function processSave(key, rethrow = false) {
  clearTimeout(saveTimers[key]);
  saveTimers[key] = null;

  if (saveInFlight[key]) {
    saveQueued[key] = true;
    return;
  }

  const task = saveTasks[key];
  if (!task) return;

  saveInFlight[key] = true;
  try {
    await task();
    if (saveIndicators[key]) {
      setSaveState(saveIndicators[key], "saved");
    }
  } catch (error) {
    console.error(error);
    showToast("تعذر الحفظ التلقائي.", true);
    if (saveIndicators[key]) {
      setSaveState(saveIndicators[key], "error");
    }
    if (rethrow) {
      throw error;
    }
  } finally {
    saveInFlight[key] = false;
    if (saveQueued[key]) {
      saveQueued[key] = false;
      void processSave(key, rethrow);
    }
  }
}

async function flushSave(key, task, indicator = null) {
  saveTasks[key] = task;
  if (indicator) {
    saveIndicators[key] = indicator;
    setSaveState(indicator, "saving");
  }
  clearTimeout(saveTimers[key]);
  saveTimers[key] = null;
  await processSave(key, true);
}

function toggleMenu(menuId) {
  const menu = document.getElementById(menuId);
  if (!menu) return;
  const alreadyOpen = menu.classList.contains("open");
  closeAllMenus();
  if (!alreadyOpen) {
    menu.classList.add("open");
  }
}

function closeAllMenus() {
  document.querySelectorAll(".dropdown.open").forEach((menu) => menu.classList.remove("open"));
}

function showToast(message, isError = false) {
  clearTimeout(toastTimer);
  refs.toast.classList.remove("hidden");
  refs.toast.textContent = message;
  refs.toast.style.background = isError ? "#6f1d18" : "#19312f";
  toastTimer = window.setTimeout(() => {
    refs.toast.classList.add("hidden");
  }, 2300);
}

function setSaveState(scope, status) {
  const map = {
    story: {
      wrap: refs.storySaveState,
      icon: refs.storySaveIcon,
      text: refs.storySaveText,
    },
    chapter: {
      wrap: refs.chapterSaveState,
      icon: refs.chapterSaveIcon,
      text: refs.chapterSaveText,
    },
  };
  const target = map[scope];
  if (!target?.wrap || !target?.icon || !target?.text) return;

  target.wrap.classList.remove("is-idle", "is-saving", "is-saved", "is-error");

  if (status === "saving") {
    target.wrap.classList.add("is-saving");
    target.icon.textContent = "…";
    target.text.textContent = "جارٍ الحفظ";
    return;
  }

  if (status === "saved") {
    target.wrap.classList.add("is-saved");
    target.icon.textContent = "✓";
    target.text.textContent = "تم الحفظ";
    return;
  }

  if (status === "error") {
    target.wrap.classList.add("is-error");
    target.icon.textContent = "!";
    target.text.textContent = "تعذر الحفظ";
    return;
  }

  target.wrap.classList.add("is-idle");
  target.icon.textContent = "•";
  target.text.textContent = "جاهز";
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function sanitizeFileName(name) {
  return (name || "document").replace(/[\\/:*?"<>|]/g, "-").trim() || "document";
}

function normalizeUsername(username) {
  return username.trim().toLowerCase();
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

async function ensureUserProfile(user) {
  const profileRef = ref(db, `users/${user.uid}/profile`);
  const profileSnap = await get(profileRef);
  const profile = profileSnap.exists() ? profileSnap.val() : null;
  const username = (user.displayName || profile?.username || emailToDefaultUsername(user.email) || "كاتب").trim() || "كاتب";
  const provider = user.providerData?.[0]?.providerId || "password";
  const email = user.email || profile?.email || "";
  const patch = {
    username,
    email,
    emailVerified: !!user.emailVerified,
    provider,
  };

  if (!profileSnap.exists()) {
    await set(profileRef, {
      ...patch,
      createdAt: Date.now(),
    });
  } else {
    const mustUpdate =
      profile?.username !== patch.username ||
      profile?.email !== patch.email ||
      profile?.emailVerified !== patch.emailVerified ||
      profile?.provider !== patch.provider;
    if (mustUpdate) {
      await update(profileRef, patch);
    }
  }

  if (provider === "password") {
    const usernameRef = ref(db, `usernames/${usernameToKey(username)}`);
    const usernameSnap = await get(usernameRef);
    const usernameData = usernameSnap.exists() ? usernameSnap.val() : null;
    if (!usernameData) {
      await set(usernameRef, {
        uid: user.uid,
        email,
        username,
        createdAt: Date.now(),
      });
    } else if (usernameData.uid === user.uid && usernameData.email !== email) {
      await update(usernameRef, { email, username });
    }
  }

  if (username) {
    usernameIndex.add(normalizeUsername(username));
  }

  if (email) {
    const emailRef = ref(db, `emails/${emailToKey(email)}`);
    const emailSnap = await get(emailRef);
    const emailData = emailSnap.exists() ? emailSnap.val() : null;
    if (!emailData) {
      await set(emailRef, {
        uid: user.uid,
        email,
        createdAt: Date.now(),
      });
    } else if (emailData.uid === user.uid && emailData.email !== email) {
      await update(emailRef, { email });
    }
  }

  if (user.displayName !== username) {
    await updateProfile(user, { displayName: username });
  }
}

async function sendVerificationForCurrentUser() {
  if (!auth.currentUser) {
    throw new Error("لا يوجد مستخدم حالي لإرسال رمز التحقق.");
  }
  const continueUrl = `${window.location.origin}${window.location.pathname}`;
  await sendEmailVerification(auth.currentUser, {
    url: continueUrl,
    handleCodeInApp: false,
  });
}

async function checkRedirectResult() {
  try {
    const result = await getRedirectResult(auth);
    if (result?.user) {
      showToast("تم تسجيل الدخول عبر Google.");
    }
  } catch (error) {
    refs.authStatus.style.color = "#b3261e";
    refs.authStatus.textContent = withErrorCode(readableError(error), error);
    console.error("Google redirect result error:", error);
  }
}

function withErrorCode(message, error) {
  const code = error?.code ? ` (${error.code})` : "";
  return `${message}${code}`;
}

async function resolveEmailByUsername(username) {
  const clean = username.trim();
  if (!clean) {
    throw new Error("اكتب اسم المستخدم أو البريد الإلكتروني.");
  }

  const snap = await get(ref(db, `usernames/${usernameToKey(clean)}`));
  if (!snap.exists()) {
    throw new Error("اسم المستخدم غير موجود.");
  }
  const data = snap.val();
  if (!data?.email) {
    throw new Error("لا يوجد بريد مرتبط بهذا الحساب.");
  }
  return String(data.email).trim().toLowerCase();
}

function isPasswordUser(user) {
  return Array.isArray(user?.providerData) && user.providerData.some((provider) => provider.providerId === "password");
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || "").trim());
}

function emailToDefaultUsername(email) {
  const local = String(email || "").split("@")[0] || "";
  return local.trim();
}

async function ensureUsernameIndex() {
  if (usernameIndexPromise) {
    return usernameIndexPromise;
  }

  usernameIndexPromise = (async () => {
    try {
      const usersSnap = await get(ref(db, "users"));
      usernameIndex.clear();
      if (usersSnap.exists()) {
        const users = usersSnap.val() || {};
        Object.values(users).forEach((userRecord) => {
          const username = userRecord?.profile?.username;
          if (username) {
            usernameIndex.add(normalizeUsername(username));
          }
        });
      }
      return true;
    } catch (error) {
      console.warn("Username index fallback failed:", error);
      return false;
    }
  })();

  return usernameIndexPromise;
}

function extractVerificationCode(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) return "";

  try {
    const parsed = new URL(value);
    const oobCode = parsed.searchParams.get("oobCode");
    if (oobCode) return oobCode;
  } catch {
    // ignore URL parse errors and try regex extraction
  }

  const queryMatch = value.match(/[?&]oobCode=([^&]+)/);
  if (queryMatch?.[1]) {
    return decodeURIComponent(queryMatch[1]);
  }

  return value;
}

function readableError(error) {
  const code = error?.code || "";
  if (error?.message && !code) return error.message;
  if (code.includes("auth/email-already-in-use")) return "هذا البريد مستخدم بالفعل.";
  if (code.includes("auth/user-not-found")) return "اسم المستخدم غير موجود.";
  if (code.includes("auth/invalid-email")) return "البريد الإلكتروني غير صالح.";
  if (code.includes("auth/invalid-credential")) return "بيانات الدخول غير صحيحة.";
  if (code.includes("auth/wrong-password")) return "كلمة المرور غير صحيحة.";
  if (code.includes("auth/popup-closed-by-user")) return "تم إغلاق نافذة Google قبل إكمال الدخول.";
  if (code.includes("auth/popup-blocked")) return "المتصفح منع نافذة Google.";
  if (code.includes("auth/account-exists-with-different-credential")) return "هذا البريد مرتبط بطريقة تسجيل دخول مختلفة.";
  if (code.includes("auth/unauthorized-domain")) return "الدومين غير مضاف في Authorized domains داخل Firebase.";
  if (code.includes("auth/invalid-login-credentials")) return "بيانات الدخول غير صحيحة.";
  if (code.includes("auth/user-disabled")) return "هذا الحساب معطّل.";
  if (code.includes("auth/requires-recent-login")) return "يرجى إعادة تسجيل الدخول.";
  if (code.includes("auth/invalid-action-code")) return "رابط الاستعادة/التحقق غير صالح أو منتهي.";
  if (code.includes("auth/operation-not-supported-in-this-environment")) return "هذا المتصفح لا يدعم طريقة تسجيل Google الحالية.";
  if (code.includes("auth/operation-not-allowed")) return "طريقة الدخول هذه غير مفعلة في Firebase Authentication.";
  if (code.includes("auth/app-not-authorized")) return "الدومين الحالي غير مضاف في Authorized domains داخل Firebase.";
  if (code.includes("auth/invalid-api-key")) return "Firebase API Key غير صحيح أو غير مفعّل.";
  if (code.includes("auth/too-many-requests")) return "محاولات كثيرة. جرّب لاحقًا.";
  if (code.includes("auth/network-request-failed")) return "تعذر الاتصال بالإنترنت.";
  if (code.includes("PERMISSION_DENIED") || code.includes("permission-denied")) return "صلاحيات قاعدة البيانات لا تسمح بهذه العملية.";
  return "حدث خطأ. حاول مرة أخرى.";
}

function ensureAuth() {
  if (!state.user) {
    throw new Error("يجب تسجيل الدخول أولًا.");
  }
  if (isPasswordUser(state.user) && !state.user.emailVerified) {
    throw new Error("يجب تأكيد البريد الإلكتروني قبل المتابعة.");
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function usernameToKey(username) {
  const normalized = normalizeUsername(username);
  const bytes = new TextEncoder().encode(normalized);
  let hashA = 2166136261;
  let hashB = 2166136261;

  for (const byte of bytes) {
    hashA ^= byte;
    hashA = Math.imul(hashA, 16777619);
    hashB ^= byte ^ 93;
    hashB = Math.imul(hashB, 16777619);
  }

  const partA = (hashA >>> 0).toString(16).padStart(8, "0");
  const partB = (hashB >>> 0).toString(16).padStart(8, "0");
  const lengthPart = bytes.length.toString(16).padStart(4, "0");
  return `u_${partA}${partB}${lengthPart}`;
}

function emailToKey(email) {
  const normalized = String(email || "").trim().toLowerCase();
  const bytes = new TextEncoder().encode(normalized);
  let hash = 2166136261;

  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, 16777619);
  }

  const partA = (hash >>> 0).toString(16).padStart(8, "0");
  return `e_${partA}_${bytes.length}`;
}
