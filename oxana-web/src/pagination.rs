use crate::JOBS_PER_PAGE;

pub(crate) struct JobPage {
    pub jobs: Vec<oxana::JobEnvelope>,
    pub number: usize,
    pub total: usize,
    pub has_next: bool,
}

impl JobPage {
    pub fn clamp_number(page: usize, total: usize) -> usize {
        page.clamp(1, total.div_ceil(JOBS_PER_PAGE).max(1))
    }

    pub fn list_opts(page: usize) -> oxana::QueueListOpts {
        oxana::QueueListOpts {
            count: JOBS_PER_PAGE + 1,
            offset: (page.max(1) - 1) * JOBS_PER_PAGE,
        }
    }

    pub fn new(page: usize, total: usize, mut jobs: Vec<oxana::JobEnvelope>) -> Self {
        let has_next = jobs.len() > JOBS_PER_PAGE;
        jobs.truncate(JOBS_PER_PAGE);
        Self {
            jobs,
            number: page.max(1),
            total,
            has_next,
        }
    }

    pub fn range_start(&self) -> usize {
        (self.number - 1) * JOBS_PER_PAGE + 1
    }

    pub fn range_end(&self) -> usize {
        ((self.number - 1) * JOBS_PER_PAGE + self.jobs.len()).min(self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::JobPage;

    #[test]
    fn page_number_tracks_last_available_page_after_deletion() {
        assert_eq!(JobPage::clamp_number(3, 101), 3);
        assert_eq!(JobPage::clamp_number(3, 100), 2);
        assert_eq!(JobPage::clamp_number(3, 0), 1);
        assert_eq!(JobPage::clamp_number(0, 100), 1);
    }
}
