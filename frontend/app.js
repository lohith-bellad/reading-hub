(() => {
    const PAGE_LIMIT = 20;
    const FEED_EL = document.getElementById("feed");
    const SENTINEL = document.getElementById("sentinel");
    const STATUS = document.getElementById("status");
    const CATEGORIES_EL = document.getElementById("categories");

    let cursor = null;
    let loading = false;
    let exhausted = false;
    let category = "";
    let renderedIds = new Set();
    let currentEpoch = 0;

    function setStatus(msg, withSpinner = false) {
        STATUS.innerHTML = withSpinner
            ? `<span class="spinner"></span>${msg}`
            : msg;
    }

    function relativeTime(iso) {
        if (!iso) return "";
        const t = new Date(iso).getTime();
        if (Number.isNaN(t)) return "";
        const diff = Date.now() - t;
        const m = Math.round(diff / 60000);
        if (m < 1)   return "just now";
        if (m < 60)  return `${m}m ago`;
        const h = Math.round(m / 60);
        if (h < 24)  return `${h}h ago`;
        const d = Math.round(h / 24);
        if (d < 30)  return `${d}d ago`;
        return new Date(t).toLocaleDateString();
    }

    function escapeHtml(s) {
        if (s == null) return "";
        return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function stripHtml(s) {
        if (!s) return "";
        const tmp = document.createElement("div");
        tmp.innerHTML = s;
        return tmp.textContent || tmp.innerText || "";
    }

    function renderCard(item) {
        const li = document.createElement("li");
        li.className = "card";

        const pub  = item.publisher || {};
        const time = relativeTime(item.published_at);
        const summary = stripHtml(item.summary).trim();

        const thumb = item.thumbnail_url
            ? `<div class="card-thumb" style="background-image:url('${escapeHtml(item.thumbnail_url)}')"></div>`
            : "";

        const trackedHref = `/api/article/${encodeURIComponent(item.id)}/click`;

        li.innerHTML = `
            <a class="card-link" href="${trackedHref}" target="_blank" rel="noopener noreferrer">
                <div class="card-body">
                    <h2 class="card-title">${escapeHtml(item.title)}</h2>
                    <div class="card-meta">
                        <span class="publisher">${escapeHtml(pub.name || "")}</span>
                        ${time ? `<span class="dot"></span><span>${escapeHtml(time)}</span>` : ""}
                        ${item.author ? `<span class="dot"></span><span>${escapeHtml(item.author)}</span>` : ""}
                    </div>
                    ${summary ? `<p class="card-summary">${escapeHtml(summary)}</p>` : ""}
                </div>
                ${thumb}
            </a>
        `;
        return li;
    }

    async function loadPage() {
        if (loading || exhausted) return;
        loading = true;
        setStatus("loading…", true);

        const epoch = currentEpoch;
        const params = new URLSearchParams({ limit: String(PAGE_LIMIT) });
        if (cursor)   params.set("cursor", cursor);
        if (category) params.set("category", category);

        try {
            const resp = await fetch(`/api/feed?${params}`);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const data = await resp.json();

            // Reset happened mid-flight; abandon this page.
            if (epoch !== currentEpoch) return;

            for (const item of data.items) {
                if (renderedIds.has(item.id)) continue;
                renderedIds.add(item.id);
                FEED_EL.appendChild(renderCard(item));
            }

            cursor = data.next_cursor;
            if (!cursor) {
                exhausted = true;
                setStatus(renderedIds.size === 0 ? "Nothing here yet." : "— end of feed —");
            } else {
                setStatus("");
            }
        } catch (err) {
            console.error(err);
            setStatus(`Failed to load: ${err.message}. Tap to retry.`);
            STATUS.style.cursor = "pointer";
            STATUS.onclick = () => { STATUS.style.cursor = ""; STATUS.onclick = null; loadPage(); };
        } finally {
            loading = false;
        }
    }

    function reset() {
        currentEpoch += 1;
        cursor = null;
        exhausted = false;
        renderedIds = new Set();
        FEED_EL.innerHTML = "";
        setStatus("");
    }

    CATEGORIES_EL.addEventListener("click", (e) => {
        const btn = e.target.closest(".cat");
        if (!btn) return;
        const next = btn.dataset.category || "";
        if (next === category) return;
        for (const b of CATEGORIES_EL.querySelectorAll(".cat")) b.classList.remove("active");
        btn.classList.add("active");
        category = next;
        reset();
        loadPage();
    });

    const io = new IntersectionObserver((entries) => {
        for (const entry of entries) {
            if (entry.isIntersecting) loadPage();
        }
    }, { rootMargin: "400px 0px" });
    io.observe(SENTINEL);

    loadPage();
})();
