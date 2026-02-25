import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  createUserWithEmailAndPassword,
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
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

const numberFormat = new Intl.NumberFormat("ar-EG");

const views = {
  auth: document.getElementById("auth-view"),
  home: document.getElementById("home-view"),
  storyEditor: document.getElementById("story-editor-view"),
  novelOverview: document.getElementById("novel-overview-view"),
  chapterEditor: document.getElementById("chapter-editor-view"),
};

const refs = {
  loginModeBtn: document.getElementById("login-mode-btn"),
  signupModeBtn: document.getElementById("signup-mode-btn"),
  authForm: document.getElementById("auth-form"),
  authUsername: document.getElementById("auth-username"),
  authPassword: document.getElementById("auth-password"),
  authConfirm: document.getElementById("auth-confirm"),
  confirmWrap: document.getElementById("confirm-wrap"),
  authSubmit: document.getElementById("auth-submit"),
  authStatus: document.getElementById("auth-status"),
  welcomeText: document.getElementById("welcome-text"),
  homeEmpty: document.getElementById("home-empty"),
  libraryList: document.getElementById("library-list"),
  storyTitleInput: document.getElementById("story-title-input"),
  storyTextarea: document.getElementById("story-textarea"),
  storyWordCount: document.getElementById("story-word-count"),
  storySceneCount: document.getElementById("story-scene-count"),
  novelTitleInput: document.getElementById("novel-title-input"),
  novelChapterCount: document.getElementById("novel-chapter-count"),
  novelWordCount: document.getElementById("novel-word-count"),
  novelSceneCount: document.getElementById("novel-scene-count"),
  chaptersList: document.getElementById("chapters-list"),
  chapterTitleInput: document.getElementById("chapter-title-input"),
  chapterTextarea: document.getElementById("chapter-textarea"),
  chapterWordCount: document.getElementById("chapter-word-count"),
  chapterSceneCount: document.getElementById("chapter-scene-count"),
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
  libraryUnsubscribe: null,
};

const saveTimers = {};
let toastTimer = null;

bindUI();
setAuthMode("login");
showView("auth");

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
    renderHome();
    refs.authPassword.value = "";
    refs.authConfirm.value = "";
    showView("auth");
    return;
  }

  const profileSnap = await get(ref(db, `users/${user.uid}/profile`));
  const profile = profileSnap.exists() ? profileSnap.val() : {};
  state.username = user.displayName || profile.username || "كاتب";
  refs.welcomeText.textContent = `مرحبًا ${state.username}`;

  const libraryRef = ref(db, `users/${user.uid}/library`);
  state.libraryUnsubscribe = onValue(libraryRef, (snapshot) => {
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
  });

  showView("home");
  renderHome();
});

function bindUI() {
  refs.loginModeBtn.addEventListener("click", () => setAuthMode("login"));
  refs.signupModeBtn.addEventListener("click", () => setAuthMode("signup"));
  refs.authForm.addEventListener("submit", handleAuthSubmit);

  refs.storyTitleInput.addEventListener("input", () => {
    scheduleSave("story", saveCurrentStory);
  });
  refs.storyTextarea.addEventListener("input", () => {
    updateStoryStats();
    scheduleSave("story", saveCurrentStory);
  });

  refs.novelTitleInput.addEventListener("input", () => {
    scheduleSave("novel-title", saveCurrentNovelTitle);
  });

  refs.chapterTitleInput.addEventListener("input", () => {
    scheduleSave("chapter", saveCurrentChapter);
  });
  refs.chapterTextarea.addEventListener("input", () => {
    updateChapterStats();
    scheduleSave("chapter", saveCurrentChapter);
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
  refs.confirmWrap.classList.toggle("hidden", !signUpMode);
  refs.authSubmit.textContent = signUpMode ? "إنشاء الحساب" : "دخول";
  refs.authStatus.textContent = "";
}

async function handleAuthSubmit(event) {
  event.preventDefault();

  const username = refs.authUsername.value.trim();
  const password = refs.authPassword.value;
  const confirm = refs.authConfirm.value;

  if (username.length < 3) {
    refs.authStatus.textContent = "اسم المستخدم يجب أن يكون 3 أحرف على الأقل.";
    return;
  }

  if (password.length < 8) {
    refs.authStatus.textContent = "كلمة المرور يجب ألا تقل عن 8 خانات.";
    return;
  }

  if (state.authMode === "signup" && password !== confirm) {
    refs.authStatus.textContent = "تأكيد كلمة المرور غير متطابق.";
    return;
  }

  refs.authStatus.style.color = "#0f4c49";
  refs.authStatus.textContent = state.authMode === "signup" ? "جارٍ إنشاء الحساب..." : "جارٍ تسجيل الدخول...";
  refs.authSubmit.disabled = true;

  try {
    if (state.authMode === "signup") {
      await signUpWithUsername(username, password);
      refs.authStatus.textContent = "تم إنشاء الحساب بنجاح.";
    } else {
      await loginWithUsername(username, password);
      refs.authStatus.textContent = "تم تسجيل الدخول.";
    }
  } catch (error) {
    refs.authStatus.style.color = "#b3261e";
    refs.authStatus.textContent = readableError(error);
  } finally {
    refs.authSubmit.disabled = false;
  }
}

async function signUpWithUsername(username, password) {
  const email = usernameToEmail(username);
  const credential = await createUserWithEmailAndPassword(auth, email, password);
  const now = Date.now();

  await Promise.all([
    updateProfile(credential.user, { displayName: username }),
    set(ref(db, `users/${credential.user.uid}/profile`), { username, createdAt: now }),
  ]);
}

async function loginWithUsername(username, password) {
  const email = usernameToEmail(username);
  const credential = await signInWithEmailAndPassword(auth, email, password);
  if (!credential.user.displayName) {
    const profileSnap = await get(ref(db, `users/${credential.user.uid}/profile`));
    const profile = profileSnap.exists() ? profileSnap.val() : null;
    if (profile?.username) {
      await updateProfile(credential.user, { displayName: profile.username });
    }
  }
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
      await flushSave("story", saveCurrentStory);
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
      await flushSave("chapter", saveCurrentChapter);
      const chapterId = await addChapter(state.currentNovelId);
      await openChapterEditor(state.currentNovelId, chapterId);
      return;
    }

    case "chapter-save-exit":
    case "chapter-back-overview":
      await flushSave("chapter", saveCurrentChapter);
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

function scheduleSave(key, task, delay = 700) {
  clearTimeout(saveTimers[key]);
  saveTimers[key] = window.setTimeout(() => {
    task().catch((error) => {
      console.error(error);
      showToast("تعذر الحفظ التلقائي.", true);
    });
  }, delay);
}

async function flushSave(key, task) {
  clearTimeout(saveTimers[key]);
  delete saveTimers[key];
  await task();
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

function usernameToEmail(username) {
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
  return `u${partA}${partB}${lengthPart}@writer.local`;
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function readableError(error) {
  const code = error?.code || "";
  if (error?.message && !code) return error.message;
  if (code.includes("auth/email-already-in-use")) return "اسم المستخدم مستخدم بالفعل.";
  if (code.includes("auth/user-not-found")) return "اسم المستخدم غير موجود.";
  if (code.includes("auth/invalid-credential")) return "بيانات الدخول غير صحيحة.";
  if (code.includes("auth/wrong-password")) return "كلمة المرور غير صحيحة.";
  if (code.includes("auth/too-many-requests")) return "محاولات كثيرة. جرّب لاحقًا.";
  if (code.includes("auth/network-request-failed")) return "تعذر الاتصال بالإنترنت.";
  return "حدث خطأ. حاول مرة أخرى.";
}

function ensureAuth() {
  if (!state.user) {
    throw new Error("يجب تسجيل الدخول أولًا.");
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
