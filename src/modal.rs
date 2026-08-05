use dioxus::prelude::*;

const MODAL_FOCUS_MANAGER_SCRIPT: &str = r#"
(() => {
  if (window.__bomModalFocusManager) return;
  const stack = [];
  const focusableSelector = 'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
  const visible = (element) => !!(element.offsetWidth || element.offsetHeight || element.getClientRects().length);
  const focusables = (modal) => Array.from(modal.querySelectorAll(focusableSelector)).filter(visible);
  const topModal = () => Array.from(document.querySelectorAll('.modal-card')).filter(visible).at(-1);
  const sync = () => {
    const modals = Array.from(document.querySelectorAll('.modal-card'));
    for (const modal of modals) {
      if (stack.some((entry) => entry.modal === modal)) continue;
      const title = modal.querySelector('h3');
      const id = `bom-modal-${crypto.randomUUID()}`;
      modal.id ||= id;
      modal.setAttribute('role', 'dialog');
      modal.setAttribute('aria-modal', 'true');
      modal.tabIndex = -1;
      if (title) {
        title.id ||= `${modal.id}-title`;
        modal.setAttribute('aria-labelledby', title.id);
      }
      const entry = { modal, returnFocus: document.activeElement };
      stack.push(entry);
      requestAnimationFrame(() => {
        if (!modal.isConnected || topModal() !== modal) return;
        const target = modal.querySelector('[autofocus]') || focusables(modal)[0] || modal;
        target.focus();
      });
    }
    for (let index = stack.length - 1; index >= 0; index--) {
      const entry = stack[index];
      if (entry.modal.isConnected) continue;
      stack.splice(index, 1);
      if (entry.returnFocus instanceof HTMLElement && entry.returnFocus.isConnected) {
        entry.returnFocus.focus();
      }
    }
  };
  const onKeyDown = (event) => {
    if (event.key !== 'Tab') return;
    const modal = topModal();
    if (!modal) return;
    const controls = focusables(modal);
    if (controls.length === 0) {
      event.preventDefault();
      modal.focus();
      return;
    }
    const first = controls[0];
    const last = controls.at(-1);
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };
  const observer = new MutationObserver(sync);
  observer.observe(document.body, { childList: true, subtree: true });
  document.addEventListener('keydown', onKeyDown, true);
  window.__bomModalFocusManager = { observer, onKeyDown };
  sync();
})();
"#;

#[component]
pub fn ModalFocusManager() -> Element {
    use_effect(|| {
        document::eval(MODAL_FOCUS_MANAGER_SCRIPT);
    });
    use_drop(|| {
        document::eval(
            "window.__bomModalFocusManager?.observer.disconnect(); document.removeEventListener('keydown', window.__bomModalFocusManager?.onKeyDown, true); delete window.__bomModalFocusManager;",
        );
    });
    rsx! {}
}
