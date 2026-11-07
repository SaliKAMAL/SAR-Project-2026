# SAR Multicast Project

**Group Contact Point:** Salma KAMAL
**Final Submission Branch:** `main`

> **Note on Repository Hosting:**
> As the group's contact point, my initial individual work at the start of the SAR course was hosted on my university GitLab account (`Salma-KAMAL`). However, for this collective final project, we established this GitHub repository under my GitHub account (`SaliKAMAL`) as our main workspace. This choice was necessary to unify our environment and facilitate technical collaboration, as all my teammates were exclusively using GitHub since the beginning of the course.

---

## 1. Documentation
The architecture and specification documents are located in the `docs/` folder:
- **Specification:** `docs/SAR_Multicast_Project_Specification.pdf`
- **System Design (Runtime & Protocols):** `docs/Design.md`

## 2. Eclipse Project
The Eclipse project is located in the **`Salma_KAMAL/`** directory.
**How to import:** In Eclipse, go to `File` → `Import` → `Existing Projects into Workspace` and select the `Salma_KAMAL/` folder. No additional configuration is required.

## 3. The Team & Contributions ("Who did what")

| Member & Email | Git Name | Personal Branch | Contribution ("Who did what") |
| :--- | :--- | :--- | :--- |
| **Soufian El Ouazzani**<br>`soufian.elouazzani@etu.univ-grenoble-alpes.fr` | `soufian-elouazzani` | `soufian` | **Part 1:** Implementation of the Task Runtime and Totally Ordered Multicast without failures (Core architecture, static group, Broker, Queues). |
| **Salma KAMAL**<br>`salma.kamal@etu.univ-grenoble-alpes.fr` | `SaliKAMAL` | `salma` | **Part 2:** Perfect failure detector (without timeouts) and absolute intersection Flush mechanism to guarantee Total Order during crashes. Resolution of simultaneous crashes. |
| **Chaimae BOUTI**<br>`Chaimae.Bouti@etu.univ-grenoble-alpes.fr` | `Chaymae888` | `chaimae` | **Part 3:** Actor resurrection (respawn) and reinsertion mechanism using flush sets for view changes with message generation increments. |
| **Luiz Eduardo Akazawa**<br>`luiz-eduardo.akazawa-nora@etu.univ-grenoble-alpes.fr` | `LuizAkazawa` | `luiz` | **Part 4:** Implementation of voluntary `join()` and `leave()` mechanisms. Adapted the flush protocol for alive members leaving without dropping pending messages. |

## 4. How to run the tests

All tests must be executed from the root of the Eclipse project (`Salma_KAMAL/`) via the terminal.

**Prerequisite (Compilation):**
Change directory to the project folder first:
```bash
cd Salma_KAMAL
javac -d bin $(find api local utils.queues tests -name "*.java")
