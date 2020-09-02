pub struct Semaphored<T> {
	t: T,
	sema: tokio::sync::Semaphore,
}

impl <T> Semaphored<T> {
	pub fn new(t: T, permits: usize) -> Semaphored<T> {
		Semaphored { t, sema: tokio::sync::Semaphore::new(permits) }
	}
    pub async fn acquire(&self) -> Permitted<'_, T> {
		Permitted { __: self.sema.acquire().await, t: &self.t }
	}
}

pub struct Permitted<'a, T> {
	__: tokio::sync::SemaphorePermit<'a>,
	t: &'a T,
}

impl <'a, T> std::ops::Deref for Permitted<'a, T> {
	type Target = T;
	fn deref(&self) -> &<Self as std::ops::Deref>::Target { self.t }
}
