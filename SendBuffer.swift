/**
* Copyright (c) 2015 jmnavarro. All rights reserved.
*
* This library is free software; you can redistribute it and/or modify it under
* the terms of the GNU Lesser General Public License as published by the Free
* Software Foundation; either version 2.1 of the License, or (at your option)
* any later version.
*
* This library is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
* FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
* details.
*/

import Foundation


public class SendBuffer<Element> {

	public var onAdded: (Element -> ())?
	public var onLocked: (Element -> ())?
	public var onRemoved: (Element -> ())?

	public var onFlush: ((
			items: [Element],
			commit: ()->(),
			rollback: ()->(),
			queue: NSOperationQueue) -> ())?

	public var currentElements: [Element] {
		return buffer
	}

	public var lockedElements: [Element] {
		return locked
	}

	public var isFull: Bool {
		return self.buffer.count >= bufferSize
	}

	public var isFlushing: Bool {
		return !self.locked.isEmpty
	}

	public var size: Int {
		return bufferSize
	}

	public var autoFlush = true

	private let bufferSize: Int
	private let bufferQueue: NSOperationQueue
	private var doFlushWhenCompleted = false

	private var buffer: [Element]
	private var locked: [Element]

	private let queueKey   = ("send-buffer-queue-key" as NSString).UTF8String
	private var queueValue = ("send-buffer-queue" as NSString).UTF8String

	public init(bufferSize: Int) {
		self.bufferSize = bufferSize

		let queueName = "sendbuffer-serial"
		let queue = dispatch_queue_create(queueName, DISPATCH_QUEUE_SERIAL)
		dispatch_queue_set_specific(queue, queueKey, &queueValue, nil)

		bufferQueue = NSOperationQueue()
		bufferQueue.name = queueName
		bufferQueue.maxConcurrentOperationCount = 1
		bufferQueue.underlyingQueue = queue

		buffer = [Element]()
		buffer.reserveCapacity(bufferSize)

		locked = [Element]()
		locked.reserveCapacity(bufferSize)
	}

	public func add(e: Element) {
		bufferQueue.addOperationWithBlock {
			self.buffer.append(e)
			self.onAdded?(e)

			if self.autoFlush {
				self.flushIfNeeded()
			}
		}
	}

	public func flush() {
		bufferQueue.addOperationWithBlock {
			if self.isFlushing {
				// if several flush calls happened from different threads
				// then a new flush will be done when the current one is completed
				self.doFlushWhenCompleted = true
				return
			}

			let itemCount = min(self.bufferSize, self.buffer.count)

			let range = 0..<itemCount
			self.locked += Array(self.buffer[range])
			self.buffer.removeRange(range)

			if let onLocked = self.onLocked {
				self.locked.forEach(onLocked)
			}

			self.onFlush?(
				items: self.locked,
				commit: self.commitFlush,
				rollback: self.rollbackFlush,
				queue: self.bufferQueue)
		}
	}

	private func commitFlush() {
		assertOnBufferQueue()

		if let onRemoved = self.onRemoved {
			locked.forEach(onRemoved)
		}

		locked.removeAll(keepCapacity: true)

		if autoFlush || doFlushWhenCompleted {
			flushIfNeeded()
			doFlushWhenCompleted = false
		}
	}

	private func rollbackFlush() {
		assertOnBufferQueue()

		// insert them back in the head. Keep same order
		locked.reverse().forEach {
			buffer.insert($0, atIndex: 0)
			onAdded?($0)
		}

		locked.removeAll(keepCapacity: true)

		doFlushWhenCompleted = false
	}

	private func flushIfNeeded() {
		if isFull {
			if isFlushing {
				// Flush is needed but the queue is flushing right now.
				// When the current flush is completed, it will start over again
				doFlushWhenCompleted = true
			}
			else {
				flush()
			}
		}
	}

	private func assertOnBufferQueue() {
		let value = dispatch_get_specific(queueKey)
		assert(value == &queueValue)
	}

}
