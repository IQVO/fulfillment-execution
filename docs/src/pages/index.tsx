import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns (WMS/WES/WCS, pull-based
      dispatch, CloudEvents). Not a production system. Not affiliated with,
      endorsed by, or representative of the reference operator or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <span className={styles.heroKicker}>
          warehouse-systems · WES tier · Core subdomain
        </span>
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <p className={styles.heroBlurb}>
          Turns released work into completed physical operations. Downstream of
          Work Planning, upstream of WCS and equipment — and built on one rule:
          the system selects work, not workers.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/overview/">
            Read the docs
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/api-reference/">
            API Reference
          </Link>
        </div>
      </div>
    </header>
  );
}

function ContextStrip() {
  return (
    <section className={styles.strip}>
      <div className="container">
        <div className={styles.stripInner}>
          <div className={styles.stripItem}>
            <span className={styles.stripLabel}>Consumes</span>
            <code>WorkReleased</code>
            <span className={styles.stripMeta}>
              from wes-work-planning on{' '}
              <code>warehouse.work-planning.events</code>
            </span>
          </div>
          <div className={styles.stripArrow} aria-hidden="true">
            →
          </div>
          <div className={styles.stripItem}>
            <span className={styles.stripLabel}>Owns</span>
            <code>Task · Station · Package</code>
            <span className={styles.stripMeta}>
              Pick / Pack / SLAM task lifecycle, leased claims
            </span>
          </div>
          <div className={styles.stripArrow} aria-hidden="true">
            →
          </div>
          <div className={styles.stripItem}>
            <span className={styles.stripLabel}>Publishes</span>
            <code>TaskCompleted</code>
            <span className={styles.stripMeta}>
              on <code>warehouse.fulfillment.events</code>, enriched with{' '}
              <code>work_unit_id</code>
            </span>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for Fulfillment Execution — the Pick/Pack/SLAM task lifecycle bounded context of the warehouse-systems platform.">
      <HomepageHeader />
      <main>
        <ContextStrip />
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
