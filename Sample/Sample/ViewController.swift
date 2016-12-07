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

import UIKit

class ViewController: UIViewController, UITableViewDataSource {

	@IBOutlet weak var bufferTable: UITableView!
	@IBOutlet weak var sendingTable: UITableView!
	@IBOutlet weak var sentTable: UITableView!

	@IBOutlet weak var flushButton: UIButton!
	@IBOutlet weak var sendingLabel: UILabel!
	@IBOutlet weak var errorOnSend: UISwitch!

	class Item: NSObject {
		let desc: String
		let ts: TimeInterval

		init(desc: String) {
			self.desc = desc
			ts = Date().timeIntervalSince1970
		}

		override var debugDescription: String {
			return desc
		}

		override var description: String {
			return desc
		}
	}

	let buffer = SendBuffer<Item>(bufferSize: 5)

	var sentItems = [Item]()
	var itemSec = 0

	@IBAction func produce(_ sender: AnyObject) {
		itemSec += 1
		let itemid = "\(self.itemSec)"

		let priority = DispatchQoS.`default`.qosClass
		DispatchQueue.global(qos: priority).async {
			self.buffer.add(Item(desc: "\(itemid)"))
			sleep(1)
		}
	}

	@IBAction func consume(_ sender: AnyObject) {
		buffer.flush()
	}

	override func viewDidLoad() {
		super.viewDidLoad()

		buffer.autoFlush = true

		bufferTable.dataSource = self
		sendingTable.dataSource = self
		sentTable.dataSource = self

		buffer.onAdded = { item in
			self.refreshUI()
		}
		buffer.onLocked = { item in
			self.refreshUI()
		}
		buffer.onRemoved = { item in
			self.sentItems.append(item)
			self.refreshUI()
		}
		buffer.onFlush = { (items, commit, rollback, queue) in
			self.refreshUI()

			let priority = DispatchQoS.`default`.qosClass
			DispatchQueue.global(qos: priority).async {

				for i in 0..<5 {
					DispatchQueue.main.async {
						self.sendingLabel.text = "Sending (\(5-i))"
					}
					sleep(1)
				}

				DispatchQueue.main.async {
					if self.errorOnSend.isOn {
						self.sendingLabel.text = "Failed"
					}
					else {
						self.sendingLabel.text = "Sent"
					}
				}

				queue.underlyingQueue!.async {
					if self.errorOnSend.isOn {
						rollback()
					}
					else {
						commit()
					}
				}
			}
		}
	}

	func refreshUI() {
		DispatchQueue.main.async {
			self.bufferTable.reloadData()
			self.sendingTable.reloadData()
			self.sentTable.reloadData()
		}
	}

	func tableArray(_ tableView: UITableView) -> [Item] {
		if tableView === bufferTable {
			return buffer.currentElements
		}
		else if tableView === sendingTable {
			return buffer.lockedElements
		}
		else {
			return sentItems
		}
	}

	func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
		return tableArray(tableView).count
	}

	func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
		let elements = tableArray(tableView)

		let cell = tableView.dequeueReusableCell(withIdentifier: "item", for: indexPath)

		if indexPath.row < elements.count {
			cell.textLabel?.text = elements[indexPath.row].description
		}
		else {
			cell.textLabel?.text = ""
		}

		return cell
	}

	@IBAction func autoFlushSwitch(_ sender: UISwitch) {
		buffer.autoFlush = sender.isOn
		if buffer.autoFlush && buffer.currentElements.count >= buffer.size {
			buffer.flush()
		}
		flushButton.isHidden = sender.isOn
	}

}

