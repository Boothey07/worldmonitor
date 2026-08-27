import { subscribeAuthState, type AuthSession } from '@/services/auth-state';
import { mountUserButton } from '@/services/clerk';
import { setTrustedHtml, trustedHtml } from '@/utils/dom-utils';

export class AuthHeaderWidget {
  private container: HTMLElement;
  private unsubscribeAuth: (() => void) | null = null;
  private unmountUserButton: (() => void) | null = null;

  private onSettingsClick?: () => void;


  constructor(
    _onSignInClick?: () => void,
    onSettingsClick?: () => void,
    _onBillingClick?: () => void,
  ) {
    this.onSettingsClick = onSettingsClick;
    this.container = document.createElement('div');
    this.container.className = 'auth-header-widget';

    this.unsubscribeAuth = subscribeAuthState((state: AuthSession) => {
      if (state.isPending) {
        this.renderPending();
        return;
      }
      this.render(state);
    });
  }

  public getElement(): HTMLElement {
    return this.container;
  }

  public destroy(): void {
    this.unmountUserButton?.();
    this.unmountUserButton = null;
    if (this.unsubscribeAuth) {
      this.unsubscribeAuth();
      this.unsubscribeAuth = null;
    }
  }

  private render(_state: AuthSession): void {
    this.unmountUserButton?.();
    this.unmountUserButton = null;
    this.container.classList.remove('auth-header-widget-pending');
    this.container.removeAttribute('aria-busy');
    setTrustedHtml(this.container, trustedHtml('', 'legacy direct innerHTML migration'));

    // Self-host: always render signed-in (no sign-in flows exist)
    this.renderSignedIn();
  }

  private renderPending(): void {
    this.unmountUserButton?.();
    this.unmountUserButton = null;
    this.container.classList.add('auth-header-widget-pending');
    this.container.setAttribute('aria-busy', 'true');
    setTrustedHtml(this.container, trustedHtml('', 'legacy direct innerHTML migration'));

    const signInSkeleton = document.createElement('span');
    signInSkeleton.className = 'auth-header-skeleton auth-header-skeleton-signin';
    signInSkeleton.setAttribute('aria-hidden', 'true');
    this.container.appendChild(signInSkeleton);

    const signUpSkeleton = document.createElement('span');
    signUpSkeleton.className = 'auth-header-skeleton auth-header-skeleton-signup';
    signUpSkeleton.setAttribute('aria-hidden', 'true');
    this.container.appendChild(signUpSkeleton);
  }


  private renderSignedIn(): void {
    const userBtnEl = document.createElement('div');
    userBtnEl.className = 'auth-clerk-user-button';
    this.container.appendChild(userBtnEl);
    this.unmountUserButton = mountUserButton(userBtnEl, {

      onSettingsClick: this.onSettingsClick,
    });
  }
}
