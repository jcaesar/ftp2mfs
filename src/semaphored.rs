pub struct Semaphored<T> {
	t: T,
	sema: tokio::sync::Semaphore,
	initial_permits: usize,
}

impl<T> Semaphored<T> {
	pub fn new(t: T, permits: usize) -> Semaphored<T> {
		Semaphored {
			t,
			sema: tokio::sync::Semaphore::new(permits),
			initial_permits: permits,
		}
	}
	pub async fn acquire(&self) -> Permitted<'_, T> {
		Permitted {
			p: self.sema.acquire().await,
			t: &self.t,
		}
	}
	pub async fn into_inner(self) -> T {
		let _ = self.sema.acquire().await;
		self.t
	}
	pub fn add_permits(&self, n: usize) {
		self.sema.add_permits(n);
	}
}

impl<T: Clone> Clone for Semaphored<T> {
	fn clone(&self) -> Self {
		Semaphored {
			t: self.t.clone(),
			sema: tokio::sync::Semaphore::new(self.initial_permits),
			initial_permits: self.initial_permits,
		}
	}
}

pub struct Permitted<'a, T> {
	p: tokio::sync::SemaphorePermit<'a>,
	t: &'a T,
}

impl<T> Permitted<'_, T> {
	pub fn forget_permit(self) {
		self.p.forget()
	}
}

impl<T> std::ops::Deref for Permitted<'_, T> {
	type Target = T;
	fn deref(&self) -> &<Self as std::ops::Deref>::Target {
		self.t
	}
}
